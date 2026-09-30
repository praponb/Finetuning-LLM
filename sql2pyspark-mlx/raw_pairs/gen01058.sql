select
    id,
    service,
    log_ts,
    level,
    latency_ms,
    status_code,
    k,
    value
from app_logs
lateral view outer explode(context) t as k, value;
