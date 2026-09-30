WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, attr_value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(service) AS service_length,
        TRIM(level) AS clean_level,
        COALESCE(key, 'NONE') AS resolved_key
    FROM flattened
    WHERE key IS NOT NULL
      AND latency_ms = 1778
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        service_length,
        resolved_key,
        DAY(log_ts) AS log_ts_day
    FROM cleaned
    WHERE log_ts > DATE '2025-06-01'
      AND key IN ('version', 'trace')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        resolved_key,
        log_ts_day,
        SUM(log_ts_day) OVER w_running AS log_ts_day_sum_15m,
        AVG(log_ts_day) OVER w_running AS log_ts_day_avg_15m
    FROM filtered
    WINDOW w_running AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    PERCENTILE_APPROX(log_ts_day, 0.95) AS log_ts_day_p95,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS duration_min,
    PERCENTILE_APPROX(log_ts_day, ARRAY(0.25, 0.5, 0.75)) AS log_ts_day_quartiles,
    APPROX_PERCENTILE(log_ts_day, 0.9) AS log_ts_day_p90,
    MIN(log_ts) AS started_at
FROM with_windows
GROUP BY 1
HAVING PERCENTILE_APPROX(log_ts_day, 0.95) > 0
ORDER BY log_ts_day_p95 DESC NULLS FIRST;
