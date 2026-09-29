with unpacked as (
    select
        id,
        latency_ms,
        context,
        get_json_object(payload_json, '$.duration') as duration_text,
        get_json_object(payload_json, '$.user.tier') as user_tier,
        size(context) as num_context,
        coalesce(context['trace'], 'unknown') as trace_or_default
    from app_logs
    where latency_ms = 733
)
select
    case when num_context >= 63 then 'PREMIUM' else 'STANDARD' end as num_context_tier,
    stddev_samp(latency_ms) as latency_ms_stddev,
    avg(num_context) as avg_num_context,
    count(distinct trace_or_default) as distinct_trace_or_defaults,
    count(*) as row_count,
    round(avg(latency_ms), 2) as avg_latency_ms
from unpacked
group by case when num_context >= 63 then 'PREMIUM' else 'STANDARD' end
order by distinct_trace_or_defaults
