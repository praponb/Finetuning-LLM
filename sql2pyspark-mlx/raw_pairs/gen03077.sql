select
    id,
    service,
    host,
    level,
    latency_ms,
    status_code,
    k,
    v
from app_logs
lateral view outer explode(context) x as k, v;
