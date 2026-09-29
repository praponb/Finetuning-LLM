with lines as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view explode(context) li as key, attr_value
),
unpacked as (
    -- Semi-structured parsing
    select
        id,
        log_ts,
        key,
        attr_value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 1) != 0 as int) as is_flagged
    from lines
),
latest as (
    -- QUALIFY-style window filter
    select *
    from unpacked
    qualify rank() over (partition by key order by log_ts desc) = 1
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when log_ts - lag(log_ts) over (partition by key order by log_ts, id, key) > interval '15' minute then 1 else 0 end as is_new_session
    from latest
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        log_ts,
        key,
        attr_value,
        status_code_high,
        is_flagged,
        sum(is_new_session) over (partition by key order by log_ts, id, key rows between unbounded preceding and current row) as island_id
    from with_gaps
)
select
    id,
    log_ts,
    key,
    attr_value,
    status_code_high,
    is_flagged,
    island_id,
    case when key rlike '^(region|version)$' then 'GROUP_A' else 'CORE' end as key_zone
from sessionized
order by key_zone desc, log_ts desc
