with unpacked as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        host,
        log_ts,
        latency_ms,
        context,
        coalesce(context['trace'], 'none') as trace_or_default,
        get_json_object(payload_json, '$.user.id') as user_id
    from app_logs
)
-- Aggregate with conditional metrics
select
    date_trunc('hour', log_ts) as hour_window,
    case when latency_ms >= 1860 then 'PREMIUM' else 'STANDARD' end as latency_ms_tier,
    count(distinct trace_or_default) as distinct_trace_or_defaults,
    approx_percentile(latency_ms, 0.95) as latency_ms_p95,
    count(*) as event_count,
    size(collect_set(host)) as num_hosts,
    grouping_id() as gid
from unpacked
group by cube (date_trunc('hour', log_ts), case when latency_ms >= 1860 then 'PREMIUM' else 'STANDARD' end)
having approx_percentile(latency_ms, 0.95) > 0
order by event_count desc nulls first;
