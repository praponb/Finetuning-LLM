WITH unpacked AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        ARRAY_CONTAINS(MAP_KEYS(context), 'region') AS has_region,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        COALESCE(context['version'], 'direct') AS version_or_default
    FROM app_logs
)
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    CASE WHEN latency_ms >= 1891 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_tier,
    MAX(log_ts) AS last_log_ts,
    COUNT(DISTINCT version_or_default) AS distinct_version_or_defaults,
    SUM(CASE WHEN host = 'h1' THEN latency_ms ELSE 0 END) AS h1_latency_ms,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    COUNT(*) AS num_rows,
    MIN(log_ts) AS first_log_ts
FROM unpacked
GROUP BY DATE_TRUNC('HOUR', log_ts), CASE WHEN latency_ms >= 1891 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING distinct_version_or_defaults >= 1
