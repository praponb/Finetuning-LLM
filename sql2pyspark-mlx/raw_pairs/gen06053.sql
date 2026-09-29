with unpacked as (
    -- Higher-order array functions
    select
        id,
        host,
        latency_ms,
        payload_json,
        context,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec,
        coalesce(context['version'], 'unknown') as version_or_default
    from app_logs
)
-- Running and moving aggregates
select
    *,
    avg(latency_ms) over w_recent as latency_ms_avg_5,
    ntile(3) over w_recent_2 as duration_sec_bucket
from unpacked
window w_recent as (partition by host order by duration_sec asc, id asc rows between 5 preceding and current row), w_recent_2 as (partition by host order by duration_sec asc, id asc)
