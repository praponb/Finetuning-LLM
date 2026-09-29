WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        payload_json,
        context,
        context['region'] AS region,
        COALESCE(context['region'], 'direct') AS region_or_default
    FROM app_logs
)
-- Grouped metrics
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    CASE WHEN latency_ms >= 1821 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_tier,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS duration_min,
    PERCENTILE_APPROX(latency_ms, 0.9) AS latency_ms_p90,
    MAX(log_ts) AS last_seen,
    MIN(log_ts) AS started_at,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms_2
FROM unpacked
GROUP BY DATE_TRUNC('DAY', log_ts), CASE WHEN latency_ms >= 1821 THEN 'HIGH' ELSE 'LOW' END
ORDER BY avg_latency_ms NULLS FIRST;
