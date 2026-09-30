with lines as (
    -- Unnest the array into one row per element
    select
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view outer explode(context) t as attr_key, value
)
-- Aggregate with conditional metrics
select
    attr_key,
    case when latency_ms >= 2127 then 'HIGH' else 'LOW' end as latency_ms_tier,
    level,
    sum(case when level = 'INFO' then status_code else 0 end) as info_status_code,
    percentile_approx(status_code, 0.9) as status_code_p90,
    min(log_ts) as started_at,
    count(*) filter (where level = 'DEBUG') as debug_rows,
    dense_rank() over (partition by attr_key order by count(*) filter (where level = 'DEBUG') desc) as debug_rows_dense_rank
from lines
group by 1, 2, 3
