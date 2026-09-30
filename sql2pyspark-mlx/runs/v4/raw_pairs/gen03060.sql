with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) li as attr_key, v
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        ceil(status_code) as status_code_whole
    from flattened
    where log_ts >= timestamp '2025-06-01 23:00:00'
      and attr_key is not null
      and attr_key <> 'trace'
),
scored as (
    -- Ranking and gap features
    select
        *,
        max(status_code_whole) over (partition by service order by cast(log_ts as bigint) range between 900 preceding and current row) as status_code_whole_max_15m,
        max(latency_ms) over (partition by level) as level_max_latency_ms,
        cume_dist() over w_ordered as status_code_whole_cume_dist,
        round(latency_ms / sum(latency_ms) over (partition by attr_key), 4) as latency_ms_share
    from cleaned
    window w_ordered as (partition by attr_key order by status_code_whole desc nulls last)
),
enriched as (
    -- Ranking and gap features
    select
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        status_code_whole,
        status_code_whole_max_15m,
        level_max_latency_ms,
        status_code_whole_cume_dist,
        latency_ms_share,
        percent_rank() over (partition by service order by status_code_whole) as status_code_whole_percent_rank,
        dense_rank() over (partition by level, attr_key order by status_code_whole_max_15m desc nulls last) as status_code_whole_max_15m_dense_rank,
        stddev_samp(status_code_whole) over (partition by level, service) as level_stddev_samp_status_code_whole
    from scored
),
grouped as (
    -- Multi-level aggregation
    select
        date_trunc('hour', log_ts) as hour_window,
        count(*) filter (where service = 'search') as search_rows,
        sort_array(collect_set(service)) as service_list,
        bit_and(latency_ms) as latency_ms_bit_and
    from enriched
    group by hour_window
)
select
    hour_window,
    search_rows,
    service_list,
    latency_ms_bit_and
from grouped
order by search_rows desc nulls last
