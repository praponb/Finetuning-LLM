WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
valid AS (
    SELECT *, COALESCE(latency_ms, 0) latency_ms_filled
    FROM flattened
    WHERE key IS NOT NULL
      AND log_ts >= TIMESTAMP '2025-06-01 12:00:00'
),
metrics AS (
    SELECT
        service,
        log_ts,
        latency_ms,
        key,
        COUNT(latency_ms_filled) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) latency_ms_filled_count_1h
    FROM valid
),
metrics_app_logs AS (
    SELECT
        service,
        latency_ms,
        key,
        SUM(latency_ms) OVER (PARTITION BY key) key_sum_latency_ms,
        RANK() OVER (PARTITION BY key ORDER BY latency_ms_filled_count_1h NULLS LAST) latency_ms_filled_count_1h_rank,
        CUME_DIST() OVER (PARTITION BY service ORDER BY latency_ms DESC NULLS LAST) latency_ms_cume_dist
    FROM metrics
)
SELECT *
FROM (
    SELECT
        service,
        key,
        key_sum_latency_ms
    FROM metrics_app_logs
) src
PIVOT (
    MAX(key_sum_latency_ms) AS value_sum,
    COUNT(1) AS item_count
    FOR key IN ('trace', 'version', 'region')
);
