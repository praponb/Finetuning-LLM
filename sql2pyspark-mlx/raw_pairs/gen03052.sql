with flattened_attrs as (
    -- Semi-structured parsing
    select
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        get_json_object(payload_json, '$.duration') as duration_text,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count
    from app_logs
    where service <> 'billing'
)
-- Multi-level aggregation
select
    date_trunc('hour', log_ts) as hour_window,
    sum(retry_count) filter (where service = 'checkout') as checkout_total_retry_count,
    round(avg(latency_ms), 2) as avg_latency_ms
from flattened_attrs
group by date_trunc('hour', log_ts)
having round(avg(latency_ms), 2) > 0
order by hour_window
