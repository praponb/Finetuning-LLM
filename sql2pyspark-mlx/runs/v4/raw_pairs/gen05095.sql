select
    id,
    service,
    latency_ms,
    status_code,
    k,
    v
from app_logs
lateral view explode(context) e as k, v
