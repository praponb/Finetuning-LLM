WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
)
SELECT
    key,
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    COUNT(*) FILTER (WHERE key = 'trace') AS trace_rows,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    SIZE(COLLECT_SET(service)) AS num_services,
    COUNT_IF(service = 'billing') AS billing_events,
    COUNT(DISTINCT key) AS distinct_keys,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list
FROM exploded
GROUP BY GROUPING SETS ((key, DATE_TRUNC('DAY', log_ts)), (key))
HAVING billing_events >= 1
ORDER BY activity_day DESC
