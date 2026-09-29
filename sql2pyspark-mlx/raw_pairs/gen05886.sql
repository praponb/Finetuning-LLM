WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        latency_ms,
        k,
        value,
        CAST((status_code & 4) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE k IN ('region', 'trace', 'version')
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(k) AS k_length
    FROM unpacked
    WHERE k <> 'version'
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(status_code_high, 0) AS status_code_high_filled
    FROM filtered
    WHERE status_code_high > 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        latency_ms,
        k,
        value,
        status_code_high,
        k_length,
        status_code_high_filled,
        LEAST(status_code_high, has_fault) AS least_status_code_high_has_fault
    FROM valid
    WHERE has_fault IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        latency_ms,
        k_length,
        status_code_high_filled,
        least_status_code_high_has_fault,
        COUNT(status_code_high) OVER (PARTITION BY k) AS k_count_status_code_high,
        MAX(latency_ms) OVER (PARTITION BY k) AS k_max_latency_ms,
        AVG(latency_ms) OVER (PARTITION BY k) AS k_avg_latency_ms
    FROM cleaned
)
-- Multi-level aggregation
SELECT
    CASE WHEN k_max_latency_ms >= 2217 THEN 'LARGE' ELSE 'SMALL' END AS k_max_latency_ms_tier,
    CASE WHEN least_status_code_high_has_fault >= 84 THEN 'HIGH' ELSE 'LOW' END AS least_status_code_high_has_fault_tier,
    COUNT(*) AS num_rows,
    ROUND(AVG(k_length), 2) AS avg_k_length,
    BIT_OR(latency_ms) AS latency_ms_bit_or,
    COUNT(*) AS cnt,
    ROUND(AVG(k_avg_latency_ms), 2) AS avg_k_avg_latency_ms,
    SUM(latency_ms) AS total_latency_ms,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN k_max_latency_ms >= 2217 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(*) DESC) AS num_rows_percent_rank
FROM scored
GROUP BY k_max_latency_ms_tier, least_status_code_high_has_fault_tier
ORDER BY avg_k_length ASC NULLS FIRST
