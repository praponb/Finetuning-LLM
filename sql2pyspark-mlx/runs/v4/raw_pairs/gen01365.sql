select
    id,
    service,
    host,
    log_ts,
    level,
    latency_ms,
    key,
    v
from app_logs
lateral view explode(context) e as key, v
