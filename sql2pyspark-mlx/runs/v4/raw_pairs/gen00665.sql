select
    id,
    service,
    host,
    level,
    key,
    attr_value
from app_logs
lateral view outer explode(context) li as key, attr_value;
