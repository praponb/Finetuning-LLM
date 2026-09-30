WITH flattened AS (
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, value
)
SELECT
    id,
    log_ts,
    level,
    value,
    AVG(latency_ms) OVER (PARTITION BY level, k) AS level_avg_latency_ms,
    COUNT(latency_ms) OVER (PARTITION BY level) AS level_count_latency_ms,
    COUNT(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_count_30m
FROM flattened
WHERE level IN ('WARN', 'DEBUG', 'ERROR');
