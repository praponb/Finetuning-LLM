WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        log_ts,
        level,
        context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        SIZE(context) AS num_context,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        COALESCE(context['region'], 'direct') AS region_or_default
    FROM app_logs
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    host,
    MAX(log_ts) AS last_log_ts,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN host = 'h3' THEN num_context ELSE 0 END) AS h3_num_context,
    SUM(CASE WHEN level = 'INFO' THEN retry_count ELSE 0 END) AS info_retry_count,
    AVG(status_code_high) AS avg_status_code_high
FROM flattened_attrs
GROUP BY 1, 2
HAVING COUNT(*) > 1
