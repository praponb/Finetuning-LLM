with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        level,
        k,
        value
    from app_logs
    lateral view explode(context) x as k, value
),
kept_keys as (
    -- Set difference on keys
    select host, level
    from flattened
    except all
    select host, level
    from app_logs
    where hour(log_ts) between 12 and 20
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from flattened s
    join kept_keys k on s.host = k.host and s.level = k.level
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        trim(host) as clean_host,
        concat(host, ', ', k) as host_k
    from verified
    where k is not null
),
valid as (
    -- Keep recent, valid rows
    select *, trim(level) as clean_level
    from base
    where k is not null
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        lower(k) as k_lower,
        coalesce(k, 'OTHER') as resolved_k
    from valid
    where k is not null
      and k is distinct from 'region'
),
cleaned_app_logs as (
    -- Basic cleansing and defaults
    select
        id,
        host,
        level,
        k,
        clean_host,
        host_k,
        clean_level,
        k_lower,
        lower(k) as k_lower_2,
        coalesce(k, 'DIRECT') as k_filled
    from cleaned
    where host in ('h1', 'h3')
)
-- Multi-level aggregation
select
    k,
    level,
    count(*) as event_count,
    sort_array(collect_set(level)) as level_list,
    count(*) as event_count_2
from cleaned_app_logs
group by k, level
having count(*) > 1
order by event_count_2 nulls first;
