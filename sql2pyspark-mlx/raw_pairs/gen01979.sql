WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
kept_keys AS (
    -- Set difference on keys
    SELECT host, level
    FROM flattened
    EXCEPT ALL
    SELECT host, level
    FROM app_logs
    WHERE status_code <> 431
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    INNER JOIN kept_keys k ON s.host = k.host AND s.level = k.level
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN host = 'h4' THEN 1 ELSE 0 END AS is_h4,
        COALESCE(key, 'NONE') AS key_clean
    FROM verified
    WHERE key IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        attr_value,
        is_h4,
        CASE WHEN service = 'billing' THEN 1 ELSE 0 END AS is_billing,
        COALESCE(key, 'N/A') AS key_filled
    FROM base
    WHERE is_h4 > 75
)
-- Multi-level aggregation
SELECT
    level,
    PERCENTILE_APPROX(latency_ms, 0.95) AS latency_ms_p95,
    COUNT(*) FILTER (WHERE key = 'trace') AS trace_rows,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    MIN(latency_ms) AS min_latency_ms,
    STDDEV_SAMP(latency_ms) AS latency_ms_stddev,
    MIN(log_ts) AS first_log_ts
FROM valid
GROUP BY level
ORDER BY level DESC NULLS FIRST;
