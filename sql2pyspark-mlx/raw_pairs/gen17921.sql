with unpacked as (
    select
        id,
        service,
        status_code,
        payload_json,
        context,
        coalesce(context['version'], 'direct') as version_or_default,
        coalesce(context['region'], 'direct') as region_or_default,
        coalesce(context['trace'], 'unknown') as trace_or_default,
        array_contains(map_keys(context), 'region') as has_region
    from app_logs
)
select
    *,
    sum(status_code) over w_part as running_sum_status_code,
    count(status_code) over w_part as running_count_status_code,
    avg(status_code) over w_part as running_avg_status_code
from unpacked
where service in ('search', 'auth')
window w_part as (partition by service order by status_code asc, id asc rows between unbounded preceding and current row);
