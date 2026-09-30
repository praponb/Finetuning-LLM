with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        latency_ms,
        key,
        value
    from app_logs
    lateral view explode(context) e as key, value
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        case when key = 'region' then 1 else 0 end as is_region,
        case when level = 'DEBUG' then 1 else 0 end as is_debug
    from flattened
    where level = 'WARN'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by key order by is_region, id, key) as row_seq,
        lag(is_debug, 2, 0) over (partition by year(log_ts), service order by latency_ms, id, key) as prev_is_debug,
        avg(latency_ms) over (partition by service order by latency_ms, id, key rows between 5 preceding and 1 preceding) as latency_ms_avg_5_prior,
        dense_rank() over (partition by key order by is_debug nulls last) as is_debug_dense_rank,
        row_number() over (partition by to_date(log_ts), service order by is_region, id, key) as seq_no
    from filtered
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(is_region, 0) as is_region_filled,
        coalesce(key, 'N/A') as key_filled
    from with_windows
    where log_ts > date '2025-06-01'
      and row_seq > 7
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(log_ts) over (partition by service order by log_ts, id, key) is null
                 or log_ts > lag(log_ts) over (partition by service order by log_ts, id, key) + interval 15 minutes then 1 else 0 end as is_new_burst
    from cleaned
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by service order by log_ts, id, key rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
filtered_app_logs as (
    -- Basic cleansing and defaults
    select
        id,
        service,
        level,
        latency_ms,
        key,
        value,
        is_region,
        is_debug,
        row_seq,
        prev_is_debug,
        latency_ms_avg_5_prior,
        seq_no,
        is_region_filled,
        is_new_burst,
        burst_id,
        to_date(log_ts) as log_ts_date,
        case when latency_ms >= 2567 then 'High' when latency_ms >= 1165 then 'Medium' else 'Low' end as latency_ms_band,
        coalesce(key, 'NONE') as key_clean
    from sessionized
    where log_ts >= date '2025-06-01'
)
-- Aggregate with conditional metrics
select
    case when latency_ms >= 1855 then 'HIGH' else 'LOW' end as latency_ms_tier,
    sort_array(collect_set(key)) as key_list,
    sort_array(collect_set(level)) as level_list,
    count_if(service = 'search') as search_events
from filtered_app_logs
group by case when latency_ms >= 1855 then 'HIGH' else 'LOW' end
order by latency_ms_tier;
