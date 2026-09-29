with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) li as key, attr_value
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        host,
        level,
        status_code,
        concat(host, ' ', level) as host_level,
        coalesce(key, 'NONE') as resolved_key
    from flattened
    where host <> 'h3'
      and key is not null
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        case when level = 'DEBUG' then 1 else 0 end as is_debug,
        round(status_code, 1) as status_code_rounded
    from cleaned
    where host = 'h4'
),
kept_keys as (
    -- Set difference on keys
    select host, level
    from valid
    except distinct
    select host, level
    from app_logs
    where service in ('checkout', 'auth', 'billing')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join kept_keys k on s.host = k.host and s.level = k.level
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over w_ordered as status_code_rounded_dense_rank,
        count(is_debug) over (partition by level) as level_count_is_debug,
        dense_rank() over w_grp as status_code_dense_rank
    from reconciled
    window w_ordered as (partition by host order by status_code_rounded desc), w_grp as (order by status_code desc)
)
select
    host,
    host_level,
    resolved_key,
    is_debug,
    status_code_rounded,
    level_count_is_debug,
    status_code_dense_rank
from with_windows
order by status_code_dense_rank desc, status_code_rounded;
