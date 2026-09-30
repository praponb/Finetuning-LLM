WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        payload_json,
        context,
        SIZE(context) AS num_context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        context['trace'] AS trace
    FROM app_logs
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    BIT_OR(status_code) AS status_code_bit_or,
    AVG(num_context) AS avg_num_context,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms,
    MIN(log_ts) AS first_log_ts
FROM unpacked
GROUP BY DATE_TRUNC('HOUR', log_ts), DATE_TRUNC('DAY', log_ts)
ORDER BY hour_window ASC NULLS LAST;
