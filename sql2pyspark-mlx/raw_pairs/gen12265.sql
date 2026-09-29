WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, value
)
-- Running and moving aggregates
SELECT
    id,
    service,
    host,
    latency_ms,
    attr_key,
    value,
    AVG(latency_ms) OVER (PARTITION BY service ORDER BY latency_ms, id, attr_key ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS latency_ms_avg_2_prior,
    NTILE(5) OVER w_hist AS latency_ms_bucket,
    SUM(latency_ms) OVER w_ordered AS cumulative_latency_ms,
    LAG(latency_ms, 2) OVER w_hist_2 AS prev_latency_ms
FROM flattened
WHERE level IN ('INFO', 'DEBUG', 'WARN')
WINDOW w_hist AS (PARTITION BY level ORDER BY latency_ms DESC, id, attr_key), w_ordered AS (ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist_2 AS (PARTITION BY host ORDER BY latency_ms, id, attr_key)
