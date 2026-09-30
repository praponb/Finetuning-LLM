WITH flattened_attrs AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        payload_json,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        COALESCE(context['version'], 'unknown') AS version_or_default
    FROM app_logs
)
SELECT
    CASE WHEN duration_sec >= 3.6 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    service,
    SUM(CASE WHEN service = 'auth' THEN duration_sec ELSE 0 END) AS auth_duration_sec,
    COUNT(*) AS num_rows,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    COUNT_IF(service = 'checkout') AS checkout_events
FROM flattened_attrs
GROUP BY CASE WHEN duration_sec >= 3.6 THEN 'LARGE' ELSE 'SMALL' END, service
HAVING COUNT_IF(service = 'checkout') > 1
ORDER BY duration_sec_tier DESC;
