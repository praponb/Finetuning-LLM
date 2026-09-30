WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, value
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2
    FROM exploded
    WHERE TO_DATE(log_ts) >= '2023-09-01'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        flag_bit2 / NULLIF(status_code_high, 0) AS flag_bit2_per_status_code_high,
        TO_DATE(log_ts) AS log_ts_date
    FROM flattened_attrs
    WHERE log_ts > TO_DATE('2025-06-01')
),
matched_keys AS (
    -- Set intersection on keys
    SELECT host
    FROM valid
    INTERSECT ALL
    SELECT host
    FROM app_logs
    WHERE host = 'h1'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.host = k.host
),
grouped AS (
    -- Grouped metrics
    SELECT
        host,
        level,
        SUM(CASE WHEN level = 'ERROR' THEN flag_bit2_per_status_code_high ELSE 0 END) AS error_flag_bit2_per_status_code_high,
        COUNT(*) FILTER (WHERE host = 'h3') AS h3_rows,
        ROUND(SUM(status_code_high) / NULLIF(COUNT(CASE WHEN host = 'h3' THEN 1 END), 0), 2) AS status_code_high_per_h3
    FROM verified
    GROUP BY GROUPING SETS ((host, level), (host), ())
)
SELECT
    host,
    level,
    h3_rows
FROM grouped
ORDER BY host DESC;
