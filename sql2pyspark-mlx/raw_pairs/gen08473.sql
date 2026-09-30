with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view explode(context) t as attr_key, v
)
select
    id,
    service,
    host,
    log_ts,
    status_code,
    attr_key,
    v
from lines;
