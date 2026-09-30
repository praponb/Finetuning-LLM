WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    MIN(log_ts) AS first_log_ts,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(service = 'search') AS search_events
FROM lines
GROUP BY DATE_TRUNC('HOUR', log_ts)
