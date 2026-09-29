with lines as (
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) e as key, attr_value
)
select
    host,
    log_ts,
    level,
    latency_ms,
    status_code,
    key,
    attr_value
from lines;
