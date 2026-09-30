WITH parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        context,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        ARRAY_CONTAINS(MAP_KEYS(context), 'trace') AS has_trace
    FROM app_logs
    WHERE host IN ('h3', 'h2')
)
-- Grouped metrics
SELECT
    host,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(log_ts) AS last_log_ts
FROM parsed
GROUP BY host
ORDER BY distinct_ids;
