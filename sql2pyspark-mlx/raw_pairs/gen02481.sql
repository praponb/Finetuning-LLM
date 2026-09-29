with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view explode(context) x as attr_key, value
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        count(latency_ms) over w_hist as running_count_latency_ms,
        avg(latency_ms) over w_hist as running_avg_latency_ms,
        min(latency_ms) over w_hist as running_min_latency_ms
    from flattened
    where attr_key in ('trace', 'region', 'version')
    window w_hist as (partition by service order by log_ts, id, attr_key rows between unbounded preceding and current row)
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(log_ts) over (partition by attr_key order by log_ts, id, attr_key) is null
                 or log_ts > lag(log_ts) over (partition by attr_key order by log_ts, id, attr_key) + interval '1' hour then 1 else 0 end as is_new_burst
    from metrics
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by attr_key order by log_ts, id, attr_key rows between unbounded preceding and current row) as session_id
    from with_gaps
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        service,
        status_code,
        attr_key,
        value,
        running_count_latency_ms,
        running_avg_latency_ms,
        running_min_latency_ms,
        session_id,
        floor(running_min_latency_ms) as running_min_latency_ms_whole
    from islands
    where status_code >= 268
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when running_min_latency_ms >= 1940 then 'High' when running_min_latency_ms >= 460 then 'Medium' else 'Low' end as running_min_latency_ms_level,
        case when running_min_latency_ms_whole >= 83 then 'High' when running_min_latency_ms_whole >= 28 then 'Medium' else 'Low' end as running_min_latency_ms_whole_band
    from cleaned
    where running_avg_latency_ms > 2200
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        status_code,
        attr_key,
        value,
        running_count_latency_ms,
        running_min_latency_ms,
        running_min_latency_ms_whole,
        running_min_latency_ms_level,
        running_min_latency_ms_whole_band,
        cast(running_min_latency_ms_whole as string) as running_min_latency_ms_whole_num,
        coalesce(attr_key, 'DIRECT') as attr_key_filled
    from valid
    where service = 'auth'
)
select
    id,
    status_code,
    attr_key,
    value,
    running_min_latency_ms,
    running_min_latency_ms_whole_band
from base
order by status_code desc nulls first, running_min_latency_ms;
