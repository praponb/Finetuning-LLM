WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, attr_value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        host,
        latency_ms,
        status_code,
        key,
        attr_value,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(latency_ms, 0) AS latency_ms_filled
    FROM parsed
    WHERE key NOT IN ('trace', 'version')
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, SUM(flag_bit4) OVER (PARTITION BY key, host) AS key_sum_flag_bit4
    FROM base
),
matched_keys AS (
    -- Set intersection on keys
    SELECT host
    FROM with_windows
    INTERSECT ALL
    SELECT host
    FROM app_logs
    WHERE level NOT IN ('WARN', 'INFO', 'DEBUG')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN matched_keys k ON s.host = k.host
),
base_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        latency_ms,
        status_code,
        key,
        attr_value,
        flag_bit4,
        status_code_high,
        latency_ms_filled,
        key_sum_flag_bit4,
        flag_bit4 - latency_ms_filled AS flag_bit4_minus_latency_ms_filled,
        COALESCE(key, 'N/A') AS key_clean
    FROM kept_rows
    WHERE host = 'h2'
)
-- Aggregate with conditional metrics
SELECT
    key,
    PERCENTILE_APPROX(latency_ms_filled, 0.5) AS latency_ms_filled_p50,
    COUNT(CASE WHEN key = 'trace' THEN 1 END) AS trace_count,
    SIZE(COLLECT_SET(key)) AS num_keys
FROM base_app_logs
GROUP BY key
HAVING PERCENTILE_APPROX(latency_ms_filled, 0.5) < 1000000
ORDER BY latency_ms_filled_p50 DESC NULLS FIRST;
