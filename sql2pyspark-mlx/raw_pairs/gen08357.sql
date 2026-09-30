with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        level,
        k,
        v
    from app_logs
    lateral view explode(context) e as k, v
)
select
    id,
    service,
    host,
    k,
    v,
    case when k rlike '^(version|region)$' then 'CORE' when k rlike '^(trace)$' then 'EXTENDED' else 'GROUP_B' end as k_zone
from lines
order by host desc nulls first
