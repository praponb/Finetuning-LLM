with unpacked as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        service,
        level,
        status_code,
        payload_json,
        context,
        coalesce(context['region'], 'unknown') as region_or_default,
        size(context) as num_context,
        shiftright(status_code, 2) as status_code_high,
        coalesce(context['version'], 'none') as version_or_default
    from app_logs
)
-- Aggregate with conditional metrics
select
    case when num_context >= 72 then 'HIGH' else 'LOW' end as num_context_tier,
    count_if(service = 'billing') as billing_events,
    count(case when level = 'WARN' then 1 end) as warn_count,
    size(collect_set(level)) as num_levels,
    count(*) as event_count,
    count_if(service = 'search') as search_events,
    grouping(case when num_context >= 72 then 'HIGH' else 'LOW' end) as grouping_id
from unpacked
group by rollup (case when num_context >= 72 then 'HIGH' else 'LOW' end)
having billing_events > 1;
