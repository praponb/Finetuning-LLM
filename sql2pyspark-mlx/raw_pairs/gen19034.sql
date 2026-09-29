with unpacked as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        service,
        level,
        latency_ms,
        payload_json,
        context,
        coalesce(context['region'], 'none') as region_or_default,
        size(context) as num_context,
        context['trace'] as trace,
        get_json_object(payload_json, '$.user.tier') as user_tier,
        context['region'] as region
    from app_logs
)
-- Running and moving aggregates
select
    id,
    service,
    latency_ms,
    num_context,
    trace,
    user_tier,
    sum(latency_ms) over (partition by service order by latency_ms, id rows between unbounded preceding and current row) as cumulative_latency_ms,
    dense_rank() over (partition by service order by num_context desc nulls last) as num_context_dense_rank,
    lag(num_context, 1) over w_grp as prev_num_context,
    row_number() over (partition by service order by num_context desc, id) as seq_no
from unpacked
window w_grp as (partition by level, service order by latency_ms, id)
