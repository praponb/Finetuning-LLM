with unpacked as (
    select
        id,
        service,
        level,
        latency_ms,
        status_code,
        context,
        coalesce(context['region'], 'unknown') as region_or_default,
        coalesce(context['version'], 'none') as version_or_default
    from app_logs
    where latency_ms <> 1936
)
select
    id,
    service,
    level,
    latency_ms,
    status_code,
    region_or_default,
    avg(status_code) over (partition by service order by status_code asc, id asc rows between 6 preceding and current row) as status_code_avg_6,
    lag(latency_ms, 2) over (partition by service, level order by latency_ms asc, id asc) as prev_latency_ms,
    row_number() over (order by status_code asc, id asc) as seq_no,
    min(latency_ms) over (partition by service) as service_min_latency_ms
from unpacked;
