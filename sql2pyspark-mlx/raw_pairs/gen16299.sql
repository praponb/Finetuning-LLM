with parsed as (
    select
        id,
        service,
        log_ts,
        level,
        status_code,
        payload_json,
        coalesce(context['version'], 'none') as version_or_default,
        size(context) as num_context
    from app_logs
)
select
    service,
    sum(case when service = 'checkout' then status_code else 0 end) as checkout_status_code,
    sum(num_context) filter (where level = 'INFO') as info_total_num_context,
    percentile_approx(status_code, 0.95) as status_code_p95,
    count(case when level = 'DEBUG' then 1 end) as debug_count,
    percentile_approx(status_code, array(0.25, 0.5, 0.75)) as status_code_quartiles
from parsed
group by service
