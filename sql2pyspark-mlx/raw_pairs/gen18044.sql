with unpacked as (
    select
        id,
        service,
        log_ts,
        level,
        context,
        get_json_object(payload_json, '$.user.tier') as user_tier,
        context['region'] as region,
        coalesce(context['trace'], 'direct') as trace_or_default
    from app_logs
    where level in ('ERROR', 'WARN', 'DEBUG')
)
select
    *,
    first_value(region) ignore nulls over w_running first_known_region,
    row_number() over (partition by level, service order by log_ts desc, id asc) event_seq
from unpacked
window w_running as (partition by level order by log_ts asc, id asc rows between unbounded preceding and current row)
