WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        COALESCE(context['version'], 'none') AS version_or_default,
        context['version'] AS version
    FROM app_logs
)
-- Grouped metrics
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    MAX(log_ts) AS ended_at,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    MAX(log_ts) AS last_seen
FROM parsed
GROUP BY DATE_TRUNC('DAY', log_ts)
ORDER BY span_minutes DESC;
