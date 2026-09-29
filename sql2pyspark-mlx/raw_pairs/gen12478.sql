with flattened as (
    select
        id,
        host,
        latency_ms,
        status_code,
        k,
        value
    from app_logs
    lateral view explode(context) t as k, value
)
select
    id,
    host,
    latency_ms,
    status_code,
    k,
    value,
    case when k rlike '^(region|version)$' then 'PRIORITY' when k rlike '^(trace)$' then 'CORE' else 'GROUP_B' end as k_zone
from flattened
