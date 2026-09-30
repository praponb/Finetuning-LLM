with parsed as (
    select
        id,
        service,
        latency_ms,
        status_code,
        coalesce(context['version'], 'direct') as version_or_default,
        cast((status_code & 4) != 0 as int) as has_fault,
        coalesce(context['region'], 'direct') as region_or_default,
        context['trace'] as trace
    from app_logs
    where latency_ms between 1292 and 1739
)
select
    case when latency_ms >= 2206 then 'LARGE' else 'SMALL' end as latency_ms_tier,
    percentile_approx(status_code, 0.9) as status_code_p90,
    count(case when service = 'search' then 1 end) as search_count,
    round(avg(latency_ms), 2) as avg_latency_ms,
    count(*) filter (where service = 'auth') as auth_rows,
    count(distinct service) as distinct_services
from parsed
group by rollup (case when latency_ms >= 2206 then 'LARGE' else 'SMALL' end)
having status_code_p90 > 0
order by avg_latency_ms nulls first
