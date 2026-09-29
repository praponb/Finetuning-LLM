with exploded as (
    select
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        v
    from app_logs
    lateral view outer explode(context) t as k, v
),
filtered as (
    select
        *,
        initcap(trim(service)) as service_title,
        cast(status_code as string) as status_code_str
    from exploded
    where k is not null
),
matched_keys as (
    select host, service
    from filtered
    except all
    select host, service
    from app_logs
    where hour(log_ts) between 10 and 14
),
verified as (
    select s.*
    from filtered s
    inner join matched_keys k on s.host = k.host and s.service = k.service
),
cleaned as (
    select
        *,
        round(status_code, 1) as status_code_rounded,
        round(latency_ms, 0) as latency_ms_rounded,
        coalesce(k, 'DIRECT') as k_filled
    from verified
    where k in ('region', 'trace')
),
metrics as (
    select
        id,
        service,
        level,
        status_code,
        k,
        service_title,
        status_code_str,
        latency_ms_rounded,
        stddev_samp(status_code_rounded) over (partition by level, host) as level_stddev_samp_status_code_rounded
    from cleaned
    where level not in ('ERROR', 'INFO', 'WARN')
),
with_windows as (
    select
        service,
        service_title,
        status_code_str,
        latency_ms_rounded,
        level_stddev_samp_status_code_rounded,
        max(latency_ms_rounded) over (partition by level) as level_max_latency_ms_rounded,
        dense_rank() over (partition by service order by latency_ms_rounded desc) as latency_ms_rounded_dense_rank,
        percent_rank() over (order by latency_ms_rounded nulls last) as latency_ms_rounded_percent_rank
    from metrics
),
metrics_app_logs as (
    select
        *,
        stddev_samp(latency_ms_rounded_percent_rank) over w_recent as service_stddev_samp_latency_ms_rounded_percent_rank,
        avg(level_max_latency_ms_rounded) over w_recent as service_avg_level_max_latency_ms_rounded
    from with_windows
    window w_recent as (partition by service)
)
select
    service,
    sum(latency_ms_rounded_percent_rank) filter (where service = 'billing') as billing_total_latency_ms_rounded_percent_rank,
    count(*) filter (where service = 'billing') as billing_rows,
    count_if(service = 'search') as search_events,
    percentile_approx(service_stddev_samp_latency_ms_rounded_percent_rank, 0.95) as service_stddev_samp_latency_ms_rounded_percent_rank_p95
from metrics_app_logs
group by service;
