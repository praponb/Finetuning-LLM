WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        level,
        latency_ms,
        payload_json,
        context,
        COALESCE(context['region'], 'direct') AS region_or_default,
        context['trace'] AS trace,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text
    FROM app_logs
)
-- Aggregate with conditional metrics
SELECT
    level,
    MAX(flag_bit2) FILTER (WHERE host = 'h2') AS h2_max_flag_bit2,
    COUNT(CASE WHEN level = 'DEBUG' THEN 1 END) AS debug_count,
    APPROX_PERCENTILE(flag_bit2, 0.9) AS flag_bit2_p90,
    ROUND(AVG(retry_count), 2) AS avg_retry_count
FROM unpacked
GROUP BY level
HAVING MAX(flag_bit2) FILTER (WHERE host = 'h2') < 1000000;
