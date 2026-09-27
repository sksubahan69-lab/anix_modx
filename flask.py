import json
import os
import threading
import time
import uuid
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import OrderedDict
from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template_string, request
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)
app.secret_key = os.urandom(32).hex()

# ═══════════════════════════════════════════════════════════
# ⚡ GLOBAL CONFIGURATION (World-Scale)
# ═══════════════════════════════════════════════════════════
MAX_WORKERS = 30              # Per-session parallel workers
GLOBAL_MAX_WORKERS = 500      # Total across all users
CONNECT_TIMEOUT = 3
READ_TIMEOUT = 5
RETRY_COUNT = 1
POOL_SIZE = 100
MAX_LOGS_PER_SESSION = 500
SESSION_TTL_MINUTES = 30      # Auto-cleanup after 30 min idle
MAX_FILE_SIZE_MB = 20         # Prevent abuse

# Global executor (shared across users for efficiency)
GLOBAL_EXECUTOR = ThreadPoolExecutor(
    max_workers=GLOBAL_MAX_WORKERS,
    thread_name_prefix="ultra_worker"
)

# ═══════════════════════════════════════════════════════════
# 🧠 MULTI-USER SESSION STORE
# ═══════════════════════════════════════════════════════════
class SessionStore:
    """Thread-safe, memory-bounded multi-user session store."""
    
    def __init__(self):
        self._lock = threading.RLock()
        self._sessions = OrderedDict()  # LRU order
        self._last_cleanup = time.time()
    
    def create(self, session_id=None):
        with self._lock:
            if not session_id:
                session_id = str(uuid.uuid4())
            
            self._sessions[session_id] = {
                "id": session_id,
                "logs": [],
                "stats": {"total": 0, "success": 0, "failed": 0, "processed": 0},
                "in_progress": False,
                "created_at": time.time(),
                "last_activity": time.time()
            }
            # Move to end (most recent)
            self._sessions.move_to_end(session_id)
            return session_id
    
    def get(self, session_id):
        with self._lock:
            session = self._sessions.get(session_id)
            if session:
                session["last_activity"] = time.time()
                self._sessions.move_to_end(session_id)
            return session
    
    def add_log(self, session_id, message, status="info"):
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return
            timestamp = time.strftime("%H:%M:%S")
            session["logs"].append({
                "time": timestamp,
                "message": message,
                "status": status
            })
            if len(session["logs"]) > MAX_LOGS_PER_SESSION:
                # Keep last 80% for performance
                session["logs"] = session["logs"][-int(MAX_LOGS_PER_SESSION * 0.8):]
    
    def update_stats(self, session_id, key, value=1):
        with self._lock:
            session = self._sessions.get(session_id)
            if session:
                session["stats"][key] = session["stats"].get(key, 0) + value
    
    def set_in_progress(self, session_id, value):
        with self._lock:
            session = self._sessions.get(session_id)
            if session:
                session["in_progress"] = value
    
    def cleanup_stale(self):
        """Remove sessions idle for > TTL. Runs periodically."""
        now = time.time()
        if now - self._last_cleanup < 60:  # At most once per minute
            return
        self._last_cleanup = now
        
        with self._lock:
            ttl_seconds = SESSION_TTL_MINUTES * 60
            stale = [
                sid for sid, s in self._sessions.items()
                if (now - s["last_activity"] > ttl_seconds 
                    and not s["in_progress"])
            ]
            for sid in stale:
                del self._sessions[sid]
            if stale:
                gc.collect()
            return len(stale)

session_store = SessionStore()


# ═══════════════════════════════════════════════════════════
# 🌐 HTTP SESSION POOL (Reusable for performance)
# ═══════════════════════════════════════════════════════════
def create_http_session():
    """High-performance HTTP session with connection pooling."""
    session = requests.Session()
    retry = Retry(
        total=RETRY_COUNT,
        backoff_factor=0.1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(
        pool_connections=POOL_SIZE,
        pool_maxsize=POOL_SIZE,
        max_retries=retry,
        pool_block=False
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "TermuxUltraEngine/3.0 (DarkMaster)",
        "Accept": "*/*",
        "Connection": "keep-alive",
    })
    return session


# ═══════════════════════════════════════════════════════════
# ⚡ WORKER FUNCTION (Per Account)
# ═══════════════════════════════════════════════════════════
def activate_account(session_id, http_session, index, uid, password):
    """Activate a single account. Fully isolated per user session."""
    
    url = f"https://rg-act-vip.vercel.app/jxe/act?uid={uid}&password={password}"
    
    try:
        response = http_session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        session_store.update_stats(session_id, "processed")
        
        if response.status_code == 200:
            preview = response.text.strip()[:60].replace("\n", " ")
            session_store.add_log(
                session_id,
                f"[{index}] ✓ UID {uid} → {preview}",
                "success"
            )
            session_store.update_stats(session_id, "success")
            return True
        else:
            session_store.add_log(
                session_id,
                f"[{index}] ✗ UID {uid} → HTTP {response.status_code}",
                "danger"
            )
            session_store.update_stats(session_id, "failed")
            return False
            
    except requests.exceptions.ConnectTimeout:
        session_store.add_log(session_id, f"[{index}] ⏱ Timeout: {uid}", "danger")
        session_store.update_stats(session_id, "failed")
        session_store.update_stats(session_id, "processed")
        return False
    except requests.exceptions.ReadTimeout:
        session_store.add_log(session_id, f"[{index}] ⏱ Read timeout: {uid}", "danger")
        session_store.update_stats(session_id, "failed")
        session_store.update_stats(session_id, "processed")
        return False
    except requests.RequestException as e:
        session_store.add_log(
            session_id,
            f"[{index}] ✗ Error: {uid} → {str(e)[:60]}",
            "danger"
        )
        session_store.update_stats(session_id, "failed")
        session_store.update_stats(session_id, "processed")
        return False


# ═══════════════════════════════════════════════════════════
# 🚀 MAIN PROCESSOR (Per User, Fully Isolated)
# ═══════════════════════════════════════════════════════════
def process_json_accounts(session_id, file_storage):
    """Process user's JSON in parallel. Non-blocking for other users."""
    
    session_store.set_in_progress(session_id, True)
    
    try:
        session_store.add_log(session_id, "⚡ ULTRA mode — multi-threaded processing", "success")
        session_store.add_log(session_id, "Parsing JSON from memory...", "info")
        
        data = json.load(file_storage)
        
        if not isinstance(data, list):
            session_store.add_log(session_id, "Invalid JSON: expected list", "danger")
            return
        
        # Extract valid accounts
        accounts = []
        for index, item in enumerate(data, start=1):
            if not isinstance(item, dict):
                continue
            uid = str(item.get("uid", "")).strip()
            password = str(item.get("password", "")).strip()
            if not uid or not password:
                session_store.add_log(session_id, f"[{index}] ⚠ Missing uid/password", "warning")
                continue
            accounts.append((index, uid, password))
        
        session_store.update_stats(session_id, "total", len(accounts))
        
        if not accounts:
            session_store.add_log(session_id, "No valid accounts found!", "warning")
            return
        
        session_store.add_log(session_id, f"🎯 {len(accounts)} valid accounts", "info")
        session_store.add_log(session_id, f"🚀 Launching {MAX_WORKERS} parallel workers", "success")
        session_store.add_log(session_id, "─" * 45, "system")
        
        start_time = time.time()
        http_session = create_http_session()
        
        # Submit to GLOBAL executor (shared, but each user gets own futures)
        futures = {
            GLOBAL_EXECUTOR.submit(
                activate_account, session_id, http_session, idx, uid, pwd
            ): (idx, uid)
            for idx, uid, pwd in accounts
        }
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                session_store.add_log(session_id, f"Worker error: {str(e)[:60]}", "danger")
        
        http_session.close()
        elapsed = time.time() - start_time
        
        stats = session_store.get(session_id)["stats"]
        session_store.add_log(session_id, "─" * 45, "system")
        session_store.add_log(
            session_id,
            f"⚡ DONE in {elapsed:.2f}s | ✓ {stats['success']} | ✗ {stats['failed']} | 📊 {stats['total']}",
            "success"
        )
        if elapsed > 0:
            speed = stats["total"] / elapsed
            session_store.add_log(session_id, f"🚀 Speed: {speed:.1f} accounts/sec", "success")
            
    except json.JSONDecodeError:
        session_store.add_log(session_id, "Invalid JSON format!", "danger")
    except Exception as e:
        session_store.add_log(session_id, f"Critical: {str(e)[:100]}", "danger")
    finally:
        session_store.set_in_progress(session_id, False)


# ═══════════════════════════════════════════════════════════
# 🌐 ROUTES
# ═══════════════════════════════════════════════════════════
@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/upload", methods=["POST"])
def upload_file():
    # Auto-cleanup stale sessions
    session_store.cleanup_stale()
    
    # Get or create user session
    session_id = request.cookies.get("session_id") or request.form.get("session_id")
    session = session_store.get(session_id) if session_id else None
    
    if not session:
        session_id = session_store.create()
    
    if session and session["in_progress"]:
        return jsonify({"status": "error", "message": "Your task is already running!"})
    
    if "file" not in request.files:
        return jsonify({"status": "error", "message": "No file attached"})
    
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"status": "error", "message": "Empty file name"})
    
    # File size check
    file.seek(0, os.SEEK_END)
    size_mb = file.tell() / (1024 * 1024)
    file.seek(0)
    if size_mb > MAX_FILE_SIZE_MB:
        return jsonify({
            "status": "error",
            "message": f"File too large (max {MAX_FILE_SIZE_MB}MB)"
        })
    
    # Reset session logs for fresh run
    session["logs"] = []
    session["stats"] = {"total": 0, "success": 0, "failed": 0, "processed": 0}
    session_store.add_log(session_id, f"$ python3 ultra.py --json {file.filename}", "info")
    
    # Launch in background
    thread = threading.Thread(
        target=process_json_accounts,
        args=(session_id, file),
        daemon=True
    )
    thread.start()
    
    response = jsonify({"status": "started", "session_id": session_id})
    response.set_cookie("session_id", session_id, max_age=SESSION_TTL_MINUTES*60, httponly=True, samesite='Lax')
    return response


@app.route("/logs", methods=["GET"])
def get_logs():
    session_id = request.cookies.get("session_id") or request.args.get("session_id")
    session = session_store.get(session_id) if session_id else None
    
    if not session:
        # Return empty state for new users (no error)
        return jsonify({
            "logs": [],
            "in_progress": False,
            "stats": {"total": 0, "success": 0, "failed": 0, "processed": 0},
            "session_id": None
        })
    
    return jsonify({
        "logs": session["logs"],
        "in_progress": session["in_progress"],
        "stats": session["stats"],
        "session_id": session_id
    })


@app.route("/health", methods=["GET"])
def health():
    """Health check for load balancers."""
    with session_store._lock:
        active = sum(1 for s in session_store._sessions.values() if s["in_progress"])
    return jsonify({
        "status": "ok",
        "active_sessions": len(session_store._sessions),
        "active_jobs": active,
        "global_workers": GLOBAL_MAX_WORKERS,
        "timestamp": datetime.utcnow().isoformat()
    })


# ═══════════════════════════════════════════════════════════
# 🎨 HTML TEMPLATE
# ═══════════════════════════════════════════════════════════
HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Termux ULTRA — JSON Activator | by Dark Master</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css">
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;700&family=Orbitron:wght@600;700;900&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #08090b; --bg-secondary: #0f1114; --bg-card: #131519;
            --bg-elevated: #1a1d22; --border-subtle: #23262c; --border-strong: #2e323a;
            --accent: #ffc107; --accent-glow: rgba(255,193,7,0.35); --accent-soft: rgba(255,193,7,0.08);
            --success: #22c55e; --danger: #ef4444; --warning: #f59e0b; --info: #3b82f6;
            --text-primary: #e8eaed; --text-secondary: #9aa0a6; --text-muted: #5f6368;
            --terminal-green: #4ade80;
        }
        * { box-sizing: border-box; }
        html { scroll-behavior: smooth; overflow-x: hidden; }
        body {
            background-color: var(--bg-primary); color: var(--text-primary);
            font-family: 'Inter', system-ui, sans-serif; font-size: 14px;
            letter-spacing: -0.01em; min-height: 100vh; min-height: 100dvh;
            margin: 0; padding: 0; overflow-x: hidden; position: relative;
            display: flex; flex-direction: column;
        }
        .bg-layer {
            position: fixed; inset: 0; z-index: -2; pointer-events: none;
            background: 
                radial-gradient(ellipse 80% 60% at 50% -10%, rgba(255,193,7,0.10), transparent 60%),
                radial-gradient(ellipse 60% 50% at 100% 100%, rgba(59,130,246,0.06), transparent 60%),
                radial-gradient(ellipse 60% 50% at 0% 50%, rgba(255,193,7,0.04), transparent 60%);
        }
        .grid-layer {
            position: fixed; inset: 0; z-index: -1; pointer-events: none;
            background-image:
                linear-gradient(rgba(255,255,255,0.018) 1px, transparent 1px),
                linear-gradient(90deg, rgba(255,255,255,0.018) 1px, transparent 1px);
            background-size: 40px 40px;
            -webkit-mask-image: radial-gradient(ellipse 100% 80% at 50% 40%, black 20%, transparent 85%);
            mask-image: radial-gradient(ellipse 100% 80% at 50% 40%, black 20%, transparent 85%);
        }
        .creator-banner {
            background: linear-gradient(90deg, rgba(255,193,7,0) 0%, rgba(255,193,7,0.15) 50%, rgba(255,193,7,0) 100%);
            border-bottom: 1px solid rgba(255,193,7,0.25); padding: 8px 0;
            text-align: center; position: relative; overflow: hidden;
            flex-shrink: 0; z-index: 2;
        }
        .creator-banner::before {
            content: ''; position: absolute; top: 0; left: -100%;
            width: 100%; height: 100%;
            background: linear-gradient(90deg, transparent, rgba(255,193,7,0.2), transparent);
            animation: sweep 3s infinite; pointer-events: none;
        }
        @keyframes sweep { 0% { left: -100%; } 100% { left: 100%; } }
        .creator-text {
            font-family: 'Orbitron', sans-serif; font-weight: 700; font-size: 12px;
            letter-spacing: 0.4em; color: var(--accent); text-transform: uppercase;
            position: relative; z-index: 1; text-shadow: 0 0 20px var(--accent-glow);
            display: inline-flex; align-items: center; gap: 12px; padding: 0 12px;
        }
        .creator-text .line { width: 30px; height: 1px; background: linear-gradient(90deg, transparent, var(--accent)); flex-shrink: 0; }
        .creator-text .line.right { background: linear-gradient(90deg, var(--accent), transparent); }
        .creator-text i { font-size: 11px; opacity: 0.8; flex-shrink: 0; }
        .navbar-custom {
            background: rgba(15,17,20,0.85); backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px); border-bottom: 1px solid var(--border-subtle);
            padding: 14px 0; position: sticky; top: 0; z-index: 100;
            flex-shrink: 0; width: 100%;
        }
        .brand-wrap { display: flex; align-items: center; gap: 12px; min-width: 0; }
        .brand-icon {
            width: 40px; height: 40px; border-radius: 10px;
            background: linear-gradient(135deg, var(--accent), #ff9800);
            display: grid; place-items: center; color: #000; font-size: 18px;
            box-shadow: 0 0 20px var(--accent-glow), inset 0 1px 0 rgba(255,255,255,0.3);
            position: relative; flex-shrink: 0;
        }
        .brand-icon::after {
            content: ''; position: absolute; inset: -2px; border-radius: 12px;
            background: linear-gradient(135deg, var(--accent), transparent);
            opacity: 0.4; z-index: -1; filter: blur(8px);
        }
        .brand-text { font-weight: 700; font-size: 15px; letter-spacing: -0.02em; color: var(--text-primary); line-height: 1.2; white-space: nowrap; }
        .brand-sub {
            font-size: 10.5px; color: var(--text-muted); font-weight: 500;
            letter-spacing: 0.08em; text-transform: uppercase;
            font-family: 'JetBrains Mono', monospace; white-space: nowrap;
        }
        .brand-sub strong { color: var(--accent); font-weight: 700; }
        .status-badge {
            display: inline-flex; align-items: center; gap: 8px;
            padding: 7px 14px; border-radius: 100px; background: var(--bg-elevated);
            border: 1px solid var(--border-strong); font-size: 12px; font-weight: 600;
            font-family: 'JetBrains Mono', monospace; transition: all 0.3s ease; flex-shrink: 0;
        }
        .status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--text-muted); flex-shrink: 0; }
        .status-badge.idle .status-dot { background: var(--text-muted); }
        .status-badge.running { border-color: rgba(34,197,94,0.4); background: rgba(34,197,94,0.08); color: var(--success); }
        .status-badge.running .status-dot { background: var(--success); box-shadow: 0 0 10px var(--success); animation: pulse 1.5s infinite; }
        .status-badge.finished { border-color: rgba(255,193,7,0.4); background: rgba(255,193,7,0.08); color: var(--accent); }
        .status-badge.finished .status-dot { background: var(--accent); box-shadow: 0 0 10px var(--accent); }
        @keyframes pulse { 0%, 100% { transform: scale(1); opacity: 1; } 50% { transform: scale(1.3); opacity: 0.7; } }
        .card-custom {
            background: var(--bg-card); border: 1px solid var(--border-subtle);
            border-radius: 16px; position: relative; overflow: hidden;
            transition: border-color 0.3s ease;
        }
        .card-custom::before {
            content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
            background: linear-gradient(90deg, transparent, var(--accent), transparent); opacity: 0.6;
        }
        .card-custom:hover { border-color: var(--border-strong); }
        .card-title-custom {
            font-size: 13px; font-weight: 700; text-transform: uppercase;
            letter-spacing: 0.1em; color: var(--accent); display: flex;
            align-items: center; gap: 10px; margin-bottom: 24px;
            font-family: 'JetBrains Mono', monospace;
        }
        .card-title-custom i { font-size: 14px; opacity: 0.9; }
        .file-upload-box {
            border: 2px dashed var(--border-strong);
            background: linear-gradient(180deg, var(--bg-secondary), var(--bg-card));
            border-radius: 14px; padding: 28px 20px; text-align: center;
            cursor: pointer; transition: all 0.3s cubic-bezier(0.4,0,0.2,1);
            position: relative; overflow: hidden;
        }
        .file-upload-box:hover { border-color: var(--accent); background: linear-gradient(180deg, var(--bg-secondary), rgba(255,193,7,0.03)); }
        .file-upload-box.dragover { border-color: var(--accent); background: rgba(255,193,7,0.06); transform: scale(1.01); }
        .file-upload-box.has-file { border-style: solid; border-color: var(--success); background: linear-gradient(180deg, var(--bg-secondary), rgba(34,197,94,0.04)); }
        .upload-icon {
            width: 56px; height: 56px; border-radius: 14px;
            background: linear-gradient(135deg, rgba(255,193,7,0.15), rgba(255,193,7,0.02));
            border: 1px solid rgba(255,193,7,0.2); display: grid; place-items: center;
            margin: 0 auto 14px; font-size: 22px; color: var(--accent); transition: transform 0.3s ease;
        }
        .file-upload-box:hover .upload-icon { transform: translateY(-3px) scale(1.05); }
        .upload-label { font-size: 14px; font-weight: 600; color: var(--text-primary); margin-bottom: 4px; }
        .upload-hint { font-size: 11.5px; color: var(--text-muted); font-family: 'JetBrains Mono', monospace; }
        .upload-hint code { color: var(--accent); background: var(--accent-soft); padding: 1px 6px; border-radius: 4px; font-size: 11px; }
        #file { display: none; }
        .file-info {
            display: none; margin-top: 14px; padding: 10px 12px;
            background: rgba(34,197,94,0.06); border: 1px solid rgba(34,197,94,0.2);
            border-radius: 8px; font-size: 12px; font-family: 'JetBrains Mono', monospace;
            color: var(--success); text-align: left; align-items: center; gap: 8px; word-break: break-all;
        }
        .file-info.show { display: flex; }
        .btn-primary-custom {
            width: 100%; padding: 14px 20px; border: none; border-radius: 11px;
            background: linear-gradient(135deg, var(--accent), #ff9800); color: #000;
            font-weight: 700; font-size: 13px; letter-spacing: 0.05em; text-transform: uppercase;
            font-family: 'JetBrains Mono', monospace; display: flex; align-items: center;
            justify-content: center; gap: 10px; cursor: pointer; transition: all 0.25s ease;
            box-shadow: 0 4px 20px rgba(255,193,7,0.15), inset 0 1px 0 rgba(255,255,255,0.3);
            position: relative; overflow: hidden;
        }
        .btn-primary-custom::before {
            content: ''; position: absolute; inset: 0;
            background: linear-gradient(135deg, transparent, rgba(255,255,255,0.3), transparent);
            transform: translateX(-100%); transition: transform 0.6s ease;
        }
        .btn-primary-custom:hover:not(:disabled)::before { transform: translateX(100%); }
        .btn-primary-custom:hover:not(:disabled) { transform: translateY(-2px); box-shadow: 0 8px 28px rgba(255,193,7,0.3); }
        .btn-primary-custom:disabled { opacity: 0.6; cursor: not-allowed; filter: grayscale(0.3); }
        .btn-ghost {
            background: transparent; border: 1px solid var(--border-strong);
            color: var(--text-secondary); padding: 6px 12px; border-radius: 8px;
            font-size: 11.5px; font-weight: 600; font-family: 'JetBrains Mono', monospace;
            cursor: pointer; transition: all 0.2s ease; display: inline-flex; align-items: center; gap: 6px;
        }
        .btn-ghost:hover { border-color: var(--accent); color: var(--accent); background: var(--accent-soft); }
        .info-alert {
            margin-top: 20px; padding: 12px 14px; background: rgba(59,130,246,0.05);
            border: 1px solid rgba(59,130,246,0.15); border-left: 3px solid var(--info);
            border-radius: 8px; font-size: 11.5px; color: var(--text-secondary);
            line-height: 1.5; display: flex; gap: 10px; align-items: flex-start;
        }
        .info-alert i { color: var(--info); margin-top: 2px; flex-shrink: 0; }
        .stats-row { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 16px; }
        .stat-item {
            background: var(--bg-secondary); border: 1px solid var(--border-subtle);
            border-radius: 10px; padding: 10px 12px; display: flex;
            align-items: center; gap: 10px; min-width: 0;
        }
        .stat-icon { width: 30px; height: 30px; border-radius: 8px; display: grid; place-items: center; font-size: 12px; flex-shrink: 0; }
        .stat-icon.total { background: rgba(59,130,246,0.12); color: var(--info); }
        .stat-icon.success { background: rgba(34,197,94,0.12); color: var(--success); }
        .stat-icon.failed { background: rgba(239,68,68,0.12); color: var(--danger); }
        .stat-info { display: flex; flex-direction: column; line-height: 1.1; min-width: 0; }
        .stat-value { font-size: 15px; font-weight: 700; font-family: 'JetBrains Mono', monospace; color: var(--text-primary); }
        .stat-label { font-size: 10px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.06em; font-weight: 600; }
        .progress-wrap { margin-bottom: 16px; display: none; }
        .progress-wrap.show { display: block; }
        .progress-label {
            display: flex; justify-content: space-between; font-size: 11px;
            font-family: 'JetBrains Mono', monospace; color: var(--text-muted);
            margin-bottom: 6px; font-weight: 500;
        }
        .progress-custom { height: 6px; background: var(--bg-elevated); border-radius: 100px; overflow: hidden; border: 1px solid var(--border-subtle); }
        .progress-fill {
            height: 100%; background: linear-gradient(90deg, var(--accent), #ff9800);
            border-radius: 100px; width: 0%; transition: width 0.3s ease;
            box-shadow: 0 0 10px var(--accent-glow); position: relative;
        }
        .progress-fill::after {
            content: ''; position: absolute; inset: 0;
            background: linear-gradient(90deg, transparent, rgba(255,255,255,0.4), transparent);
            animation: shimmer 1.2s infinite;
        }
        @keyframes shimmer { 0% { transform: translateX(-100%); } 100% { transform: translateX(100%); } }
        .terminal-container {
            background: #050607; border: 1px solid var(--border-subtle);
            border-radius: 12px; overflow: hidden; flex-grow: 1;
            display: flex; flex-direction: column; min-height: 0;
            box-shadow: inset 0 0 60px rgba(0,0,0,0.5);
        }
        .terminal-header {
            display: flex; align-items: center; gap: 8px; padding: 10px 14px;
            background: linear-gradient(180deg, #131519, #0f1114);
            border-bottom: 1px solid var(--border-subtle); flex-shrink: 0;
        }
        .terminal-dot { width: 11px; height: 11px; border-radius: 50%; flex-shrink: 0; }
        .terminal-dot.red { background: #ff5f57; }
        .terminal-dot.yellow { background: #febc2e; }
        .terminal-dot.green { background: #28c840; }
        .terminal-title {
            flex: 1; text-align: center; font-size: 11px;
            font-family: 'JetBrains Mono', monospace; color: var(--text-muted);
            font-weight: 500; white-space: nowrap; overflow: hidden;
            text-overflow: ellipsis; padding: 0 8px;
        }
        .termux-console {
            background: #050607; color: var(--terminal-green); height: 100%;
            min-height: 400px; max-height: 560px; overflow-y: auto;
            overflow-x: hidden; font-family: 'JetBrains Mono', monospace;
            padding: 16px 18px; font-size: 12.5px; line-height: 1.7; word-break: break-word;
        }
        .termux-console::-webkit-scrollbar { width: 8px; }
        .termux-console::-webkit-scrollbar-track { background: transparent; }
        .termux-console::-webkit-scrollbar-thumb { background: var(--border-strong); border-radius: 100px; border: 2px solid #050607; }
        .termux-console::-webkit-scrollbar-thumb:hover { background: var(--accent); }
        .log-line { display: flex; gap: 10px; padding: 2px 0; animation: logFadeIn 0.2s ease; }
        @keyframes logFadeIn { from { opacity: 0; transform: translateX(-4px); } to { opacity: 1; transform: translateX(0); } }
        .log-time { color: var(--text-muted); flex-shrink: 0; font-size: 11.5px; opacity: 0.7; }
        .log-msg { word-break: break-word; }
        .log-info { color: #7dd3fc; }
        .log-success { color: var(--terminal-green); font-weight: 500; }
        .log-warning { color: var(--warning); }
        .log-danger { color: var(--danger); font-weight: 500; }
        .log-system { color: var(--text-muted); font-style: italic; }
        .log-credit { color: var(--accent); font-weight: 700; letter-spacing: 0.05em; text-shadow: 0 0 8px var(--accent-glow); }
        .footer-custom {
            margin-top: auto; padding: 22px 0; border-top: 1px solid var(--border-subtle);
            background: rgba(15,17,20,0.6); backdrop-filter: blur(10px);
            position: relative; z-index: 2; flex-shrink: 0;
        }
        .footer-content { display: flex; align-items: center; justify-content: center; flex-direction: column; gap: 8px; text-align: center; }
        .footer-credit {
            font-family: 'Orbitron', sans-serif; font-weight: 700; font-size: 14px;
            letter-spacing: 0.35em; text-transform: uppercase;
            background: linear-gradient(90deg, #ffc107, #ff9800, #ffc107);
            background-size: 200% auto; -webkit-background-clip: text;
            background-clip: text; -webkit-text-fill-color: transparent;
            animation: shine 3s linear infinite; display: inline-flex;
            align-items: center; gap: 12px; padding: 0 12px;
        }
        @keyframes shine { to { background-position: 200% center; } }
        .footer-credit i {
            background: none; -webkit-text-fill-color: var(--accent); color: var(--accent);
            font-size: 12px; filter: drop-shadow(0 0 6px var(--accent-glow)); flex-shrink: 0;
        }
        .footer-sub { font-size: 11px; color: var(--text-muted); font-family: 'JetBrains Mono', monospace; letter-spacing: 0.05em; padding: 0 12px; }
        .footer-sub .divider { margin: 0 8px; opacity: 0.4; }
        .main-wrapper { position: relative; z-index: 1; flex: 1 0 auto; width: 100%; }
        .container { animation: pageFadeIn 0.6s ease; position: relative; z-index: 1; max-width: 1400px; width: 100%; }
        @keyframes pageFadeIn { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }
        .live-indicator {
            display: inline-flex; align-items: center; gap: 6px;
            font-size: 10px; color: var(--success);
            font-family: 'JetBrains Mono', monospace;
            text-transform: uppercase; letter-spacing: 0.1em;
        }
        .live-dot {
            width: 6px; height: 6px; border-radius: 50%; background: var(--success);
            box-shadow: 0 0 8px var(--success);
            animation: pulse 1.5s infinite;
        }
        @media (max-width: 991px) {
            .termux-console { min-height: 340px; max-height: 420px; }
            .brand-text { font-size: 13.5px; }
            .brand-sub { display: none; }
        }
        @media (max-width: 575px) {
            .stats-row { grid-template-columns: 1fr 1fr 1fr; gap: 6px; }
            .stat-item { padding: 8px; gap: 6px; }
            .stat-icon { width: 26px; height: 26px; font-size: 11px; }
            .stat-value { font-size: 13px; }
            .stat-label { font-size: 9px; }
            .termux-console { font-size: 11.5px; padding: 12px; }
            .card-body { padding: 18px !important; }
            .status-badge span:not(.status-dot) { display: none; }
            .status-badge { padding: 8px; }
            .creator-text { font-size: 10px; letter-spacing: 0.25em; gap: 8px; }
            .creator-text .line { width: 15px; }
            .footer-credit { font-size: 12px; letter-spacing: 0.25em; gap: 8px; }
        }
        @media (min-height: 900px) { .main-wrapper { padding: 8px 0; } }
        @media (display-mode: fullscreen) { body { overflow-y: auto; } }
    </style>
</head>
<body>
    <div class="bg-layer"></div>
    <div class="grid-layer"></div>
    <div class="creator-banner">
        <div class="creator-text">
            <span class="line"></span>
            <i class="fa-solid fa-crown"></i>
            CREATE BY DARK MASTER
            <i class="fa-solid fa-crown"></i>
            <span class="line right"></span>
        </div>
    </div>
    <nav class="navbar-custom">
        <div class="container-fluid px-4">
            <div class="d-flex align-items-center justify-content-between w-100 gap-3">
                <div class="brand-wrap">
                    <div class="brand-icon"><i class="fa-solid fa-bolt"></i></div>
                    <div>
                        <div class="brand-text">Termux ULTRA <span style="color:var(--accent);font-size:11px;">v3.0</span></div>
                        <div class="brand-sub">by <strong>DARK MASTER</strong></div>
                    </div>
                </div>
                <div id="system-status" class="status-badge idle">
                    <span class="status-dot"></span>
                    <span>System Idle</span>
                </div>
            </div>
        </div>
    </nav>
    <div class="main-wrapper">
        <div class="container my-4 my-lg-5 px-3 px-lg-4">
            <div class="row g-4">
                <div class="col-lg-4">
                    <div class="card card-custom h-100">
                        <div class="card-body p-4 d-flex flex-column">
                            <div class="card-title-custom">
                                <i class="fa-solid fa-sliders"></i>
                                <span>Control Panel</span>
                            </div>
                            <form id="upload-form" class="d-flex flex-column flex-grow-1">
                                <div class="file-upload-box mb-4" id="uploadBox">
                                    <input type="file" id="file" name="file" accept=".json" required>
                                    <div class="upload-icon"><i class="fa-solid fa-file-code"></i></div>
                                    <div class="upload-label">Drop JSON file or click to browse</div>
                                    <div class="upload-hint mt-2">Requires <code>uid</code> & <code>password</code> fields</div>
                                    <div class="file-info" id="fileInfo">
                                        <i class="fa-solid fa-check-circle"></i>
                                        <span id="fileName">file.json</span>
                                    </div>
                                </div>
                                <button type="submit" id="start-btn" class="btn-primary-custom">
                                    <i class="fa-solid fa-bolt"></i>
                                    <span>Start Ultra Engine</span>
                                </button>
                                <div class="info-alert mt-auto">
                                    <i class="fa-solid fa-shield-halved"></i>
                                    <span>Multi-user ULTRA mode: <strong>30 parallel workers per user</strong>. All data processed in memory. Auto-cleanup after 30 min idle.</span>
                                </div>
                            </form>
                        </div>
                    </div>
                </div>
                <div class="col-lg-8">
                    <div class="card card-custom h-100">
                        <div class="card-body p-4 d-flex flex-column">
                            <div class="d-flex justify-content-between align-items-center mb-3 flex-wrap gap-2">
                                <div class="card-title-custom mb-0">
                                    <i class="fa-solid fa-desktop"></i>
                                    <span>Live Console</span>
                                    <span class="live-indicator ms-2">
                                        <span class="live-dot"></span> LIVE
                                    </span>
                                </div>
                                <button type="button" class="btn-ghost" onclick="clearConsole()">
                                    <i class="fa-solid fa-eraser"></i>
                                    <span>Clear</span>
                                </button>
                            </div>
                            <div class="stats-row">
                                <div class="stat-item">
                                    <div class="stat-icon total"><i class="fa-solid fa-list-ol"></i></div>
                                    <div class="stat-info">
                                        <span class="stat-value" id="statTotal">0</span>
                                        <span class="stat-label">Total</span>
                                    </div>
                                </div>
                                <div class="stat-item">
                                    <div class="stat-icon success"><i class="fa-solid fa-check"></i></div>
                                    <div class="stat-info">
                                        <span class="stat-value" id="statSuccess">0</span>
                                        <span class="stat-label">Success</span>
                                    </div>
                                </div>
                                <div class="stat-item">
                                    <div class="stat-icon failed"><i class="fa-solid fa-xmark"></i></div>
                                    <div class="stat-info">
                                        <span class="stat-value" id="statFailed">0</span>
                                        <span class="stat-label">Failed</span>
                                    </div>
                                </div>
                            </div>
                            <div class="progress-wrap" id="progressWrap">
                                <div class="progress-label">
                                    <span>Processing</span>
                                    <span id="progressPct">0%</span>
                                </div>
                                <div class="progress-custom">
                                    <div class="progress-fill" id="progressFill"></div>
                                </div>
                            </div>
                            <div class="terminal-container">
                                <div class="terminal-header">
                                    <span class="terminal-dot red"></span>
                                    <span class="terminal-dot yellow"></span>
                                    <span class="terminal-dot green"></span>
                                    <span class="terminal-title">darkmaster@termux: ~/ultra-engine</span>
                                </div>
                                <div id="termux-screen" class="termux-console">
                                    <div class="log-line">
                                        <span class="log-time">[00:00:00]</span>
                                        <span class="log-msg log-credit">╔══════════════════════════════════════╗</span>
                                    </div>
                                    <div class="log-line">
                                        <span class="log-time">[00:00:00]</span>
                                        <span class="log-msg log-credit">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;CREATE BY DARK MASTER&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
                                    </div>
                                    <div class="log-line">
                                        <span class="log-time">[00:00:00]</span>
                                        <span class="log-msg log-credit">╚══════════════════════════════════════╝</span>
                                    </div>
                                    <div class="log-line">
                                        <span class="log-time">[00:00:00]</span>
                                        <span class="log-msg log-system">$ ultra-activator-engine v3.0 initialized. Awaiting input...</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <footer class="footer-custom">
        <div class="container px-3 px-lg-4">
            <div class="footer-content">
                <div class="footer-credit">
                    <i class="fa-solid fa-crown"></i>
                    CREATE BY DARK MASTER
                    <i class="fa-solid fa-crown"></i>
                </div>
                <div class="footer-sub">
                    Termux ULTRA v3.0 <span class="divider">•</span> Multi-User Scale <span class="divider">•</span> © 2024
                </div>
            </div>
        </div>
    </footer>
    <script>
        const uploadForm = document.getElementById('upload-form');
        const termuxScreen = document.getElementById('termux-screen');
        const systemStatus = document.getElementById('system-status');
        const startBtn = document.getElementById('start-btn');
        const fileInput = document.getElementById('file');
        const uploadBox = document.getElementById('uploadBox');
        const fileInfo = document.getElementById('fileInfo');
        const fileName = document.getElementById('fileName');
        const progressWrap = document.getElementById('progressWrap');
        const progressFill = document.getElementById('progressFill');
        const progressPct = document.getElementById('progressPct');
        const statTotal = document.getElementById('statTotal');
        const statSuccess = document.getElementById('statSuccess');
        const statFailed = document.getElementById('statFailed');
        let isRunning = false;
        let pollInterval = null;

        uploadBox.addEventListener('click', () => fileInput.click());
        uploadBox.addEventListener('dragover', (e) => { e.preventDefault(); uploadBox.classList.add('dragover'); });
        uploadBox.addEventListener('dragleave', () => uploadBox.classList.remove('dragover'));
        uploadBox.addEventListener('drop', (e) => {
            e.preventDefault();
            uploadBox.classList.remove('dragover');
            if (e.dataTransfer.files.length) {
                fileInput.files = e.dataTransfer.files;
                updateFileUI();
            }
        });
        fileInput.addEventListener('change', updateFileUI);

        function updateFileUI() {
            if (fileInput.files.length > 0) {
                const f = fileInput.files[0];
                fileName.textContent = f.name + ' (' + (f.size / 1024).toFixed(1) + ' KB)';
                fileInfo.classList.add('show');
                uploadBox.classList.add('has-file');
            } else {
                fileInfo.classList.remove('show');
                uploadBox.classList.remove('has-file');
            }
        }

        uploadForm.addEventListener('submit', async function(e) {
            e.preventDefault();
            if (isRunning) return;
            if (fileInput.files.length === 0) {
                addSystemLog('Please select a JSON file first.', 'danger');
                return;
            }
            const formData = new FormData();
            formData.append('file', fileInput.files[0]);
            startBtn.disabled = true;
            startBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i><span>Running ULTRA...</span>';
            progressWrap.classList.add('show');
            updateProgress(0);
            try {
                const response = await fetch('/upload', { 
                    method: 'POST', 
                    body: formData,
                    credentials: 'same-origin'
                });
                const data = await response.json();
                if (data.status === 'started') {
                    startPolling();
                } else {
                    addSystemLog('Error: ' + data.message, 'danger');
                    resetButton();
                    progressWrap.classList.remove('show');
                }
            } catch (err) {
                addSystemLog('Server connection failed.', 'danger');
                resetButton();
                progressWrap.classList.remove('show');
            }
        });

        function startPolling() {
            isRunning = true;
            setStatus('running', 'Executing ULTRA...');
            
            pollInterval = setInterval(async () => {
                try {
                    const res = await fetch('/logs', { credentials: 'same-origin' });
                    const data = await res.json();
                    updateTerminal(data.logs);
                    if (data.stats) {
                        statTotal.textContent = data.stats.total || 0;
                        statSuccess.textContent = data.stats.success || 0;
                        statFailed.textContent = data.stats.failed || 0;
                        if (data.stats.total > 0) {
                            updateProgress(Math.round((data.stats.processed || 0) / data.stats.total * 100));
                        }
                    }
                    if (!data.in_progress) {
                        clearInterval(pollInterval);
                        pollInterval = null;
                        isRunning = false;
                        setStatus('finished', 'Completed');
                        resetButton();
                        updateProgress(100);
                        setTimeout(() => progressWrap.classList.remove('show'), 2500);
                    }
                } catch (e) {
                    console.error('Polling error', e);
                }
            }, 500);
        }

        function updateTerminal(logs) {
            let html = '';
            logs.forEach(log => {
                let cssClass = 'log-info';
                if (log.status === 'success') cssClass = 'log-success';
                else if (log.status === 'warning') cssClass = 'log-warning';
                else if (log.status === 'danger') cssClass = 'log-danger';
                else if (log.status === 'system') cssClass = 'log-system';
                html += `<div class="log-line">
                            <span class="log-time">[${log.time}]</span>
                            <span class="log-msg ${cssClass}">${escapeHtml(log.message)}</span>
                         </div>`;
            });
            termuxScreen.innerHTML = html;
            termuxScreen.scrollTop = termuxScreen.scrollHeight;
        }

        function addSystemLog(msg, status = 'info') {
            const time = new Date().toTimeString().split(' ')[0];
            const line = document.createElement('div');
            line.className = 'log-line';
            line.innerHTML = `<span class="log-time">[${time}]</span>
                              <span class="log-msg log-${status}">${escapeHtml(msg)}</span>`;
            termuxScreen.appendChild(line);
            termuxScreen.scrollTop = termuxScreen.scrollHeight;
        }

        function escapeHtml(str) {
            return String(str).replace(/[&<>"']/g, s => ({
                '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
            }[s]));
        }

        function clearConsole() {
            const time = new Date().toTimeString().split(' ')[0];
            termuxScreen.innerHTML = `
                <div class="log-line">
                    <span class="log-time">[${time}]</span>
                    <span class="log-msg log-credit">CREATE BY DARK MASTER</span>
                </div>
                <div class="log-line">
                    <span class="log-time">[${time}]</span>
                    <span class="log-msg log-system">$ console cleared.</span>
                </div>`;
        }

        function setStatus(state, text) {
            systemStatus.className = 'status-badge ' + state;
            systemStatus.innerHTML = `<span class="status-dot"></span><span>${text}</span>`;
        }

        function resetButton() {
            startBtn.disabled = false;
            startBtn.innerHTML = '<i class="fa-solid fa-bolt"></i><span>Start Ultra Engine</span>';
        }

        function updateProgress(pct) {
            progressFill.style.width = pct + '%';
            progressPct.textContent = pct + '%';
        }

        // Resume polling if user refreshes while job is running
        window.addEventListener('load', async () => {
            try {
                const res = await fetch('/logs', { credentials: 'same-origin' });
                const data = await res.json();
                if (data.in_progress) {
                    startPolling();
                }
            } catch (e) {}
        });
    </script>
</body>
</html>
"""


# ═══════════════════════════════════════════════════════════════════════════
#   ██████╗  █████╗ ██████╗ ██╗  ██╗    ███╗   ███╗ █████╗ ███████╗████████╗███████╗██████╗
#   ██╔══██╗██╔══██╗██╔══██╗██║ ██╔╝    ████╗ ████║██╔══██╗██╔════╝╚══██╔══╝██╔════╝██╔══██╗
#   ██║  ██║███████║██████╔╝█████╔╝     ██╔████╔██║███████║███████╗   ██║   █████╗  ██████╔╝
#   ██║  ██║██╔══██║██╔══██╗██╔═██╗     ██║╚██╔╝██║██╔══██║╚════██║   ██║   ██╔══╝  ██╔══██╗
#   ██████╔╝██║  ██║██║  ██║██║  ██╗    ██║ ╚═╝ ██║██║  ██║███████║   ██║   ███████╗██║  ██║
#   ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝    ╚═╝     ╚═╝╚═╝  ╚═╝╚══════╝   ╚═╝   ╚══════╝╚═╝  ╚═╝
#
#        DARK MASTER  //  GUEST GENERATOR v8  //  NEXUS LUXURY EDITION
#        Creator : DARK MASTER
#        Build   : NEXUS
# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # For development
    app.run(debug=False, threaded=True, host="0.0.0.0", port=5000)


# For production (gunicorn):
# gunicorn -w 4 -k gthread --threads 100 --timeout 300 -b 0.0.0.0:5000 app:app