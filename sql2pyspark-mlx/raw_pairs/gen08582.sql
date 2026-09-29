with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        latency_ms,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view outer explode(context) x as attr_key, value
)
-- Running and moving aggregates
select
    status_code,
    value,
    rank() over w_part status_code_rank,
    corr(latency_ms, status_code) over (partition by service) corr_latency_ms_status_code
from flattened
where latency_ms > 2365
window w_part as (order by status_code desc);
