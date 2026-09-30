WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        payload_json,
        context,
        COALESCE(context['version'], 'unknown') AS version_or_default,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count
    FROM app_logs
)
-- Grouped metrics
SELECT
    level,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    CORR(latency_ms, status_code) AS corr_latency_ms_status_code,
    COUNT(*) AS event_count
FROM flattened_attrs
GROUP BY level
ORDER BY level;
