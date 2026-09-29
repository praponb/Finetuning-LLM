with lines as (
    select
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    from app_logs
    lateral view outer explode(context) t as k, value
),
with_windows as (
    select
        id,
        service,
        value,
        stddev_samp(latency_ms) over (partition by level) as level_stddev_samp_latency_ms,
        round(latency_ms / sum(latency_ms) over (partition by service), 4) as latency_ms_share,
        corr(latency_ms, status_code) over (partition by k, level) as corr_latency_ms_status_code
    from lines
),
remaining_keys as (
    select id, service
    from with_windows
    intersect all
    select id, service
    from app_logs
    where host = 'h1'
),
verified as (
    select s.*
    from with_windows s
    join remaining_keys k on s.id = k.id and s.service = k.service
),
cleaned as (
    select
        id,
        level_stddev_samp_latency_ms,
        corr_latency_ms_status_code,
        corr_latency_ms_status_code * latency_ms_share as corr_latency_ms_status_code_times_latency_ms_share
    from verified
    where service = 'billing'
),
grouped as (
    select
        case when corr_latency_ms_status_code >= 0.69 then 'PREMIUM' else 'STANDARD' end as corr_latency_ms_status_code_tier,
        percentile_approx(level_stddev_samp_latency_ms, array(0.25, 0.5, 0.75)) as level_stddev_samp_latency_ms_quartiles,
        round(avg(level_stddev_samp_latency_ms), 2) as avg_level_stddev_samp_latency_ms,
        approx_percentile(corr_latency_ms_status_code_times_latency_ms_share, array(0.25, 0.5, 0.75)) as corr_latency_ms_status_code_times_latency_ms_share_quartiles,
        count(*) as row_count,
        approx_percentile(corr_latency_ms_status_code, 0.95) as corr_latency_ms_status_code_p95
    from cleaned
    group by case when corr_latency_ms_status_code >= 0.69 then 'PREMIUM' else 'STANDARD' end
)
select
    corr_latency_ms_status_code_tier,
    level_stddev_samp_latency_ms_quartiles,
    row_count,
    corr_latency_ms_status_code_p95
from grouped
order by row_count desc nulls first
