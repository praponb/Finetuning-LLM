with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        level,
        latency_ms,
        key,
        value
    from app_logs
    lateral view explode(context) x as key, value
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat_ws('/', host, level) as host_level_key,
        coalesce(key, 'UNKNOWN') as key_clean
    from flattened
    where level = 'ERROR'
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        host,
        latency_ms,
        key,
        key_clean,
        concat_ws('-', level, host) as level_host_key,
        coalesce(key, 'NONE') as key_clean_2
    from filtered
    where level not in ('ERROR', 'INFO', 'WARN')
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (order by latency_ms, id, key) as row_seq,
        max(latency_ms) over (partition by key) as key_max_latency_ms,
        lead(latency_ms, 1) over (partition by host order by latency_ms, id, key) as next_latency_ms
    from valid
),
valid_app_logs as (
    -- Keep recent, valid rows
    select
        id,
        host,
        key,
        key_clean,
        level_host_key,
        row_seq,
        key_max_latency_ms,
        next_latency_ms,
        concat_ws(',', host, key) as host_key_key,
        coalesce(key, 'UNKNOWN') as resolved_key
    from scored
    where next_latency_ms between 1126 and 2274
)
select
    id,
    key,
    level_host_key,
    row_seq,
    key_max_latency_ms,
    next_latency_ms,
    case when key rlike '^(trace|version)$' then 'GROUP_A' when key rlike '^(region)$' then 'CORE' else 'EXTENDED' end as key_zone
from valid_app_logs
order by id desc nulls first;
