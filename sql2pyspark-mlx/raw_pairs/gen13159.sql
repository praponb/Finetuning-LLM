WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, v
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_code >= 429 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
    ROUND(SUM(status_code) / NULLIF(COUNT(CASE WHEN level = 'DEBUG' THEN 1 END), 0), 2) AS status_code_per_debug,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS duration_min,
    MIN(log_ts) AS first_log_ts,
    MAX(latency_ms) AS max_latency_ms,
    PERCENTILE_APPROX(latency_ms, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_quartiles
FROM lines
GROUP BY 1
