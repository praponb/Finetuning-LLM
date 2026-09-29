WITH parsed AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        context,
        context['version'] AS version,
        COALESCE(context['version'], 'direct') AS version_or_default,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
    FROM app_logs
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    MIN(log_ts) AS first_log_ts,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list
FROM parsed
GROUP BY ROLLUP (DATE_TRUNC('DAY', log_ts), DATE_TRUNC('HOUR', log_ts))
ORDER BY activity_day DESC;
