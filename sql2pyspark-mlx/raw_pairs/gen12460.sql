with flattened_attrs as (
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        context,
        coalesce(context['region'], 'direct') as region_or_default,
        cast((status_code & 2) != 0 as int) as is_flagged,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec,
        coalesce(context['version'], 'none') as version_or_default
    from app_logs
)
select
    case when is_flagged >= 1 then 'PREMIUM' else 'STANDARD' end as is_flagged_tier,
    count(case when service = 'search' then 1 end) as search_count,
    size(collect_set(host)) as num_hosts,
    bit_and(latency_ms) as latency_ms_bit_and,
    approx_percentile(duration_sec, 0.9) as duration_sec_p90
from flattened_attrs
group by rollup (case when is_flagged >= 1 then 'PREMIUM' else 'STANDARD' end)
having search_count > 1
order by search_count asc;
