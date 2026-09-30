with unpacked as (
    select
        id,
        level,
        context,
        context['trace'] as trace,
        coalesce(context['trace'], 'unknown') as trace_or_default,
        context['version'] as version,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count
    from app_logs
)
select
    case when retry_count >= 4 then 'HIGH' else 'LOW' end as retry_count_tier,
    count(*) as row_count,
    approx_percentile(duration_sec, 0.9) as duration_sec_p90,
    count(case when level = 'DEBUG' then 1 end) as debug_count,
    count(*) filter (where level = 'ERROR') as error_rows,
    sum(case when level = 'DEBUG' then retry_count else 0 end) as debug_retry_count,
    size(collect_set(level)) as num_levels
from unpacked
group by 1
order by num_levels nulls last;
