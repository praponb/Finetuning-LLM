with flattened as (
    select
        id,
        service,
        host,
        level,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) li as key, attr_value
)
select
    id,
    service,
    host,
    status_code,
    case when service rlike '^(auth|billing)$' then 'GROUP_B' when service rlike '^(search|checkout)$' then 'EXTENDED' else 'GROUP_A' end as service_zone
from flattened
order by host desc
