with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        level,
        status_code,
        key,
        v
    from app_logs
    lateral view explode(context) e as key, v
)
select
    id,
    service,
    host,
    level,
    key,
    v
from flattened
order by key nulls first;
