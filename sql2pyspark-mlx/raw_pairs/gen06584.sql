with flattened as (
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view outer explode(context) li as attr_key, value
),
metrics as (
    select
        *,
        corr(status_code, latency_ms) over (partition by host) as corr_status_code_latency_ms,
        cume_dist() over (order by status_code desc) as status_code_cume_dist
    from flattened
),
enriched as (
    select
        id,
        service,
        status_code,
        value,
        corr_status_code_latency_ms,
        status_code_cume_dist,
        sum(status_code_cume_dist) over (partition by attr_key, service order by cast(log_ts as bigint) range between 7200 preceding and current row) as status_code_cume_dist_sum_2h
    from metrics
),
enriched_app_logs as (
    select
        *,
        percent_rank() over w_running as status_code_percent_rank,
        dense_rank() over w_running_2 as status_code_cume_dist_sum_2h_dense_rank,
        stddev_samp(corr_status_code_latency_ms) over (partition by service) as service_stddev_samp_corr_status_code_latency_ms
    from enriched
    window w_running as (partition by service order by status_code desc), w_running_2 as (partition by service order by status_code_cume_dist_sum_2h desc nulls first)
),
metrics_app_logs as (
    select
        *,
        avg(status_code_percent_rank) over (partition by service) as service_avg_status_code_percent_rank,
        dense_rank() over (order by service_stddev_samp_corr_status_code_latency_ms nulls first) as service_stddev_samp_corr_status_code_latency_ms_dense_rank
    from enriched_app_logs
),
scored as (
    select
        id,
        status_code,
        value,
        corr_status_code_latency_ms,
        status_code_cume_dist,
        status_code_cume_dist_sum_2h,
        status_code_percent_rank,
        status_code_cume_dist_sum_2h_dense_rank,
        service_stddev_samp_corr_status_code_latency_ms,
        service_stddev_samp_corr_status_code_latency_ms_dense_rank,
        cume_dist() over (order by service_stddev_samp_corr_status_code_latency_ms_dense_rank desc nulls last) as service_stddev_samp_corr_status_code_latency_ms_dense_rank_cume_dist
    from metrics_app_logs
    where status_code_percent_rank < 0.45
),
cleaned as (
    select *, floor(status_code_cume_dist) as status_code_cume_dist_whole
    from scored
    where service_stddev_samp_corr_status_code_latency_ms_dense_rank_cume_dist > 0.28
)
select
    case when corr_status_code_latency_ms >= 0.67 then 'HIGH' else 'LOW' end as corr_status_code_latency_ms_tier,
    case when service_stddev_samp_corr_status_code_latency_ms_dense_rank >= 7 then 'PREMIUM' else 'STANDARD' end as service_stddev_samp_corr_status_code_latency_ms_dense_rank_tier,
    approx_percentile(service_stddev_samp_corr_status_code_latency_ms_dense_rank_cume_dist, array(0.25, 0.5, 0.75)) as service_stddev_samp_corr_status_code_latency_ms_dense_rank_cume_dist_quartiles,
    min(service_stddev_samp_corr_status_code_latency_ms_dense_rank) as min_service_stddev_samp_corr_status_code_latency_ms_dense_rank,
    round(avg(status_code_percent_rank), 2) as avg_status_code_percent_rank,
    grouping_id() as grouping_id
from cleaned
group by grouping sets ((case when corr_status_code_latency_ms >= 0.67 then 'HIGH' else 'LOW' end, case when service_stddev_samp_corr_status_code_latency_ms_dense_rank >= 7 then 'PREMIUM' else 'STANDARD' end), (case when corr_status_code_latency_ms >= 0.67 then 'HIGH' else 'LOW' end), ())
having avg_status_code_percent_rank >= 0
order by corr_status_code_latency_ms_tier desc nulls last
