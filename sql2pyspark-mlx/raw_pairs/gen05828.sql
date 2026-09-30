WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, value
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    MAX(latency_ms) AS max_latency_ms,
    COUNT(DISTINCT id) AS distinct_ids
FROM lines
GROUP BY DATE_TRUNC('HOUR', log_ts)
