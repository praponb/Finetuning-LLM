WITH flattened AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
)
SELECT
    id,
    log_ts,
    level,
    latency_ms,
    k,
    value,
    SUM(latency_ms) OVER (PARTITION BY k, level ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_latency_ms,
    SUM(latency_ms) OVER (PARTITION BY k ORDER BY latency_ms, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_latency_ms,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host, k), 4) AS latency_ms_share
FROM flattened
