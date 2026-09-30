WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        payload_json,
        ARRAY_CONTAINS(MAP_KEYS(context), 'version') AS has_version,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        SIZE(context) AS num_context,
        CAST((status_code & 4) != 0 AS INT) AS has_fault
    FROM app_logs
    WHERE status_code BETWEEN 327 AND 416
)
-- Running and moving aggregates
SELECT
    id,
    service,
    status_code,
    duration_text,
    num_context,
    has_fault,
    SUM(status_code) OVER (PARTITION BY service ORDER BY latency_ms, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code,
    FIRST_VALUE(latency_ms, TRUE) OVER (PARTITION BY service ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_latency_ms,
    FIRST_VALUE(latency_ms) IGNORE NULLS OVER (PARTITION BY service ORDER BY num_context, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_latency_ms_2
FROM parsed
WHERE service IN ('search', 'auth');
