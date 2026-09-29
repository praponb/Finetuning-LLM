with exploded as (
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        k,
        value
    from app_logs
    lateral view outer explode(context) e as k, value
),
cleaned as (
    select
        id,
        host,
        log_ts,
        value,
        case when host = 'h3' then 1 else 0 end is_h3
    from exploded
    where log_ts > to_date('2025-06-01')
),
metrics as (
    select
        id,
        host,
        log_ts,
        value,
        is_h3,
        cume_dist() over (order by is_h3 desc nulls first) is_h3_cume_dist,
        avg(is_h3) over (partition by month(log_ts), host order by cast(log_ts as bigint) asc range between 3600 preceding and current row) is_h3_avg_1h,
        sum(is_h3) over (partition by host) host_sum_is_h3
    from cleaned
),
enriched as (
    select
        id,
        is_h3,
        is_h3_avg_1h,
        avg(is_h3_cume_dist) over (order by cast(log_ts as bigint) asc range between 1800 preceding and current row) is_h3_cume_dist_avg_30m,
        percent_rank() over (partition by to_date(log_ts), host order by host_sum_is_h3 asc nulls first) host_sum_is_h3_percent_rank,
        rank() over (order by is_h3_cume_dist desc) is_h3_cume_dist_rank,
        stddev_samp(is_h3_avg_1h) over (partition by host) host_stddev_samp_is_h3_avg_1h,
        sum(is_h3_avg_1h) over (partition by host) host_sum_is_h3_avg_1h
    from metrics
),
stats as (
    select
        case when is_h3_cume_dist_rank >= 9 then 'HIGH' else 'LOW' end as is_h3_cume_dist_rank_tier,
        case when is_h3 >= 76 then 'HIGH' else 'LOW' end as is_h3_tier,
        percentile_approx(host_stddev_samp_is_h3_avg_1h, array(0.25, 0.5, 0.75)) as host_stddev_samp_is_h3_avg_1h_quartiles,
        percentile_approx(host_stddev_samp_is_h3_avg_1h, 0.5) as host_stddev_samp_is_h3_avg_1h_p50,
        sum(host_sum_is_h3_percent_rank) as total_host_sum_is_h3_percent_rank,
        grouping(case when is_h3_cume_dist_rank >= 9 then 'HIGH' else 'LOW' end) as grouping_level
    from enriched
    group by cube (case when is_h3_cume_dist_rank >= 9 then 'HIGH' else 'LOW' end, case when is_h3 >= 76 then 'HIGH' else 'LOW' end)
    having total_host_sum_is_h3_percent_rank > 0
)
select
    is_h3_cume_dist_rank_tier,
    host_stddev_samp_is_h3_avg_1h_quartiles,
    host_stddev_samp_is_h3_avg_1h_p50,
    total_host_sum_is_h3_percent_rank,
    grouping_level
from stats
order by is_h3_cume_dist_rank_tier asc
