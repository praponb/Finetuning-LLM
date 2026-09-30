WITH unpacked AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        payload_json,
        COALESCE(context['trace'], 'unknown') AS trace_or_default,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        COALESCE(context['version'], 'unknown') AS version_or_default,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec
    FROM app_logs
)
SELECT
    id,
    service,
    trace_or_default,
    duration_text,
    MAX(latency_ms) OVER (PARTITION BY host ORDER BY latency_ms, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_latency_ms,
    AVG(latency_ms) OVER (PARTITION BY host ORDER BY latency_ms, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_latency_ms,
    SUM(latency_ms) OVER (PARTITION BY host ORDER BY latency_ms, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_latency_ms,
    COUNT(latency_ms) OVER (PARTITION BY host ORDER BY latency_ms, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_latency_ms,
    ROW_NUMBER() OVER (PARTITION BY host, service ORDER BY duration_sec DESC, id) event_seq
FROM unpacked
WHERE service = 'search';
