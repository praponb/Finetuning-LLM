with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) e as attr_key, v
)
select
    id,
    service,
    host,
    attr_key,
    v,
    case when attr_key rlike '^(trace|region)$' then 'PRIORITY' else 'GROUP_B' end as attr_key_zone
from exploded;
