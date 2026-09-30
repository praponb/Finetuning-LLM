with lines as (
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
        attr_value
    from app_logs
    lateral view explode(context) li as attr_key, attr_value
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        rank() over (partition by service order by status_code asc) as status_code_rank,
        sum(latency_ms) over (partition by host order by status_code asc, id asc, attr_key asc rows between unbounded preceding and current row) as cumulative_latency_ms,
        sum(status_code) over w_hist as cumulative_status_code
    from lines
    where attr_key in ('trace', 'region')
    window w_hist as (partition by month(log_ts), attr_key order by log_ts asc, id asc, attr_key asc rows between unbounded preceding and current row)
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by level, attr_key order by cumulative_status_code desc nulls last, id asc, attr_key asc) as rank_in_group
    from scored
    qualify rank_in_group = 1
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when log_ts - lag(log_ts) over (partition by service order by log_ts asc, id asc, attr_key asc) > interval '30' minute then 1 else 0 end as is_new_burst
    from latest
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by service order by log_ts asc, id asc, attr_key asc rows between unbounded preceding and current row) as island_id
    from flagged
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        status_code_rank,
        cumulative_latency_ms,
        cumulative_status_code,
        island_id,
        upper(level) as level_upper,
        least(rank_in_group, cumulative_status_code) as least_rank_in_group_cumulative_status_code,
        coalesce(attr_key, 'NONE') as attr_key_filled
    from sessionized
    where log_ts > date '2025-06-01'
)
-- Grouped metrics
select
    date_trunc('hour', log_ts) as hour_window,
    sum(latency_ms) filter (where attr_key = 'region') as region_total_latency_ms,
    round(avg(status_code), 2) as avg_status_code,
    count(case when level = 'INFO' then 1 end) as info_count,
    count(distinct id) as distinct_ids,
    bit_or(latency_ms) as latency_ms_bit_or,
    count(case when attr_key = 'region' then 1 end) as region_count
from base
group by cube (date_trunc('hour', log_ts))
order by avg_status_code desc;
