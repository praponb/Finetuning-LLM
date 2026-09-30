with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) t as k, attr_value
)
-- Ranking and gap features
select
    host,
    log_ts,
    latency_ms,
    status_code,
    attr_value,
    percent_rank() over (order by status_code) as status_code_percent_rank,
    corr(status_code, latency_ms) over (partition by date_trunc('MONTH', log_ts), service) as corr_status_code_latency_ms
from exploded
where host = 'h4'
