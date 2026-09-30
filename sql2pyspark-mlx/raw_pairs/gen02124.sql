with unpacked as (
    -- Higher-order array functions
    select
        id,
        host,
        level,
        latency_ms,
        status_code,
        payload_json,
        context['version'] as version,
        get_json_object(payload_json, '$.user.id') as user_id,
        context['trace'] as trace
    from app_logs
)
-- Running and moving aggregates
select
    id,
    host,
    status_code,
    version,
    user_id,
    lead(latency_ms) over (partition by host order by latency_ms, id) as next_latency_ms,
    lag(latency_ms) over (partition by host order by latency_ms, id) as prev_latency_ms,
    lead(status_code) over (partition by host order by latency_ms, id) as next_status_code,
    row_number() over (partition by host order by latency_ms, id) as row_seq
from unpacked;
