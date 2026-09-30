with unpacked as (
    -- Higher-order array functions
    select
        id,
        service,
        host,
        level,
        latency_ms,
        coalesce(context['version'], 'unknown') as version_or_default,
        array_contains(map_keys(context), 'version') as has_version,
        get_json_object(payload_json, '$.duration') as duration_text
    from app_logs
    where service = 'search'
)
-- Multi-level aggregation
select
    service,
    level,
    size(collect_set(level)) as num_levels,
    round(avg(latency_ms), 2) as avg_latency_ms,
    count(case when host = 'h3' then 1 end) as h3_count,
    count_if(service = 'billing') as billing_events,
    size(collect_set(host)) as num_hosts,
    count(*) as cnt
from unpacked
group by grouping sets ((service, level), (service), ())
order by num_levels desc
