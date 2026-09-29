WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, attr_value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        status_code,
        attr_value,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE status_code BETWEEN 275 AND 314
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN flag_bit1 >= 1 THEN 'High' WHEN flag_bit1 >= 0 THEN 'Medium' ELSE 'Low' END AS flag_bit1_tier,
        ROUND(status_code_high * 1.1, 2) AS adjusted_status_code_high
    FROM parsed
    WHERE status_code BETWEEN 348 AND 359
),
base_app_logs AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(status_code) AS status_code_whole
    FROM base
    WHERE status_code <= 413
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_code_whole >= 70 THEN 'HIGH' ELSE 'LOW' END AS status_code_whole_tier,
    AVG(status_code_whole) AS avg_status_code_whole,
    COUNT(*) AS cnt,
    ROUND(AVG(status_code_high), 2) AS avg_status_code_high,
    APPROX_PERCENTILE(status_code_high, 0.5) AS status_code_high_p50,
    APPROX_PERCENTILE(status_code_high, 0.9) AS status_code_high_p90
FROM base_app_logs
GROUP BY CASE WHEN status_code_whole >= 70 THEN 'HIGH' ELSE 'LOW' END
