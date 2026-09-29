with lines as (
    select
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    from app_logs
    lateral view outer explode(context) e as key, v
),
with_windows as (
    select
        id,
        service,
        latency_ms,
        status_code,
        key,
        v,
        avg(status_code) over (partition by service order by cast(log_ts as bigint) range between 900 preceding and current row) as status_code_avg_15m,
        dense_rank() over (partition by month(log_ts), key order by latency_ms nulls first) as latency_ms_dense_rank
    from lines
),
cleaned as (
    select
        service,
        latency_ms,
        status_code,
        key,
        status_code_avg_15m - latency_ms_dense_rank as status_code_avg_15m_minus_latency_ms_dense_rank
    from with_windows
    where key is distinct from 'region'
      and latency_ms_dense_rank = 5
),
metrics as (
    select
        *,
        cume_dist() over (partition by service order by status_code_avg_15m_minus_latency_ms_dense_rank desc) as status_code_avg_15m_minus_latency_ms_dense_rank_cume_dist,
        stddev_samp(status_code_avg_15m_minus_latency_ms_dense_rank) over (partition by key) as key_stddev_samp_status_code_avg_15m_minus_latency_ms_dense_rank,
        stddev_samp(latency_ms) over w_running as service_stddev_samp_latency_ms,
        min(latency_ms) over (partition by service, key) as service_min_latency_ms,
        percent_rank() over (partition by key order by latency_ms nulls first) as latency_ms_percent_rank
    from cleaned
    window w_running as (partition by service)
)
select
    case when service_min_latency_ms >= 2595 then 'PREMIUM' else 'STANDARD' end as service_min_latency_ms_tier,
    sort_array(collect_set(key)) as key_list,
    count_if(key = 'version') as version_events,
    count_if(service = 'search') as search_events,
    sum(case when key = 'version' then service_stddev_samp_latency_ms else 0 end) as version_service_stddev_samp_latency_ms,
    round(sum(latency_ms_percent_rank) / nullif(count(case when service = 'billing' then 1 end), 0), 2) as latency_ms_percent_rank_per_billing
from metrics
group by case when service_min_latency_ms >= 2595 then 'PREMIUM' else 'STANDARD' end
order by latency_ms_percent_rank_per_billing desc;
