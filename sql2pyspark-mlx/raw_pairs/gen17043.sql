with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        level,
        k,
        value
    from app_logs
    lateral view outer explode(context) t as k, value
),
remaining_keys as (
    -- Set intersection on keys
    select level, id
    from flattened
    intersect
    select level, id
    from app_logs
    where hour(log_ts) between 10 and 16
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from flattened s
    inner join remaining_keys k on s.level = k.level and s.id = k.id
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        concat_ws(',', k, level) as k_level_key,
        substring(k, 1, 3) as k_prefix
    from reconciled
    where k in ('version', 'region', 'trace')
),
matched_keys as (
    -- Set intersection on keys
    select level, id
    from valid
    intersect all
    select level, id
    from app_logs
    where level in ('INFO', 'ERROR', 'DEBUG')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join matched_keys k on s.level = k.level and s.id = k.id
),
base as (
    -- Basic cleansing and defaults
    select *, case when k in ('version', 'region') then 'Group A' else 'Other' end as k_group
    from verified
    where k is not null
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        trim(level) as clean_level,
        coalesce(k, 'N/A') as k_clean
    from base
    where k is distinct from 'version'
      and level != 'DEBUG'
)
select
    id,
    k,
    k_level_key,
    clean_level
from cleaned
order by id desc nulls first, k desc
