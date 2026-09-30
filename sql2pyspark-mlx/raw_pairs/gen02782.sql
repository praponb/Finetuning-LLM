select
    id,
    service,
    latency_ms,
    status_code,
    attr_key,
    attr_value
from app_logs
lateral view explode(context) t as attr_key, attr_value
