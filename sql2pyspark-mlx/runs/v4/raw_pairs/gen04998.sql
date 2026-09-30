with lines as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view explode(context) e as attr_key, v
),
parsed as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        attr_key,
        v,
        cast((status_code & 4) != 0 as int) as flag_bit4,
        shiftright(status_code, 2) as status_code_high
    from lines
),
prev_events as (
    -- Previous event time per key
    select *, lag(log_ts) over (partition by attr_key order by log_ts, id, attr_key) as prev_log_ts
    from parsed
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_log_ts is null or log_ts > prev_log_ts + interval 45 minutes then 1 else 0 end as session_start_flag
    from prev_events
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by attr_key order by log_ts, id, attr_key rows between unbounded preceding and current row) as session_no
    from gaps
),
gaps_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(log_ts) over (partition by attr_key order by log_ts, id, attr_key) is null
                 or log_ts > lag(log_ts) over (partition by attr_key order by log_ts, id, attr_key) + interval '20' minute then 1 else 0 end as is_new_session
    from sessionized
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by attr_key order by log_ts, id, attr_key rows between unbounded preceding and current row) as island_id
    from gaps_2
)
-- Window metrics over several different partitions
select
    id,
    host,
    log_ts,
    level,
    latency_ms,
    attr_key,
    v,
    flag_bit4,
    status_code_high,
    prev_log_ts,
    session_start_flag,
    session_no,
    is_new_session,
    island_id,
    first_value(session_start_flag, true) over (partition by attr_key order by island_id, id, attr_key rows between unbounded preceding and current row) first_known_session_start_flag,
    avg(session_no) over w_ordered session_no_avg_3,
    ntile(4) over (partition by attr_key, level order by session_no, id, attr_key) session_no_bucket
from islands
window w_ordered as (partition by attr_key order by island_id, id, attr_key rows between 3 preceding and current row);
