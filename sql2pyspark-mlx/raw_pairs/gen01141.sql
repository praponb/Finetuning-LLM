select
    id,
    service,
    log_ts,
    level,
    key,
    value
from app_logs
lateral view explode(context) e as key, value;
