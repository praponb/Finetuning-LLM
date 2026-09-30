with exploded as (
    select
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view outer explode(context) t as attr_key, attr_value
)
select
    case when status_code >= 460 then 'LARGE' else 'SMALL' end as status_code_tier,
    level,
    case when latency_ms >= 2679 then 'HIGH' else 'LOW' end as latency_ms_tier,
    round(avg(status_code), 2) as avg_status_code,
    sum(case when level = 'DEBUG' then status_code else 0 end) as debug_status_code,
    (unix_timestamp(max(log_ts)) - unix_timestamp(min(log_ts))) / 60 as span_minutes,
    size(collect_set(level)) as num_levels,
    approx_percentile(latency_ms, 0.9) as latency_ms_p90
from exploded
group by grouping sets ((case when status_code >= 460 then 'LARGE' else 'SMALL' end, level, case when latency_ms >= 2679 then 'HIGH' else 'LOW' end), (case when status_code >= 460 then 'LARGE' else 'SMALL' end, level))
