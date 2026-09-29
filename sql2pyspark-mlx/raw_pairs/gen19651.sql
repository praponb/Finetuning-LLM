select
    id,
    service,
    log_ts,
    level,
    latency_ms,
    status_code,
    key,
    v
from app_logs
lateral view outer explode(context) li as key, v
