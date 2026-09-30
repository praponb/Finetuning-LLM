with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        level,
        attr_key,
        value
    from app_logs
    lateral view outer explode(context) x as attr_key, value
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        level,
        attr_key,
        value,
        case when host in ('h3', 'h1') then 'Core' else 'Other' end as host_group,
        coalesce(attr_key, 'UNKNOWN') as attr_key_filled
    from flattened
    where attr_key is not null
      and attr_key is distinct from 'trace'
),
filtered_app_logs as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        level,
        attr_key,
        substring(attr_key, 1, 4) as attr_key_prefix,
        coalesce(attr_key, 'DIRECT') as attr_key_clean
    from filtered
    where attr_key is not null
      and attr_key = 'version'
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        attr_key,
        attr_key_prefix,
        attr_key_clean,
        lower(attr_key) as attr_key_lower
    from filtered_app_logs
    where attr_key in ('version', 'trace')
),
filtered_app_logs_2 as (
    -- Keep recent, valid rows
    select
        id,
        attr_key,
        attr_key_prefix,
        attr_key_clean,
        attr_key_lower,
        lower(attr_key) as attr_key_lower_2
    from base
    where attr_key = 'region'
)
select
    attr_key,
    attr_key_clean,
    attr_key_lower
from filtered_app_logs_2
order by attr_key desc nulls first
