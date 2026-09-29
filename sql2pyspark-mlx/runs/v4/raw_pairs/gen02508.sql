with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        k,
        value
    from app_logs
    lateral view explode(context) x as k, value
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(k, 'unassigned') as k_filled,
        initcap(trim(k)) as k_title,
        coalesce(k, 'OTHER') as k_clean
    from exploded
    where k in ('version', 'region')
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        k,
        value,
        k_title,
        trim(k) as clean_k,
        length(k) as k_length
    from base
    where k in ('region', 'version')
)
select
    id,
    k,
    value,
    k_title,
    k_length,
    case when k rlike '^(trace|region)$' then 'CORE' else 'EXTENDED' end as k_zone
from filtered
order by k_length desc;
