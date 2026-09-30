WITH flattened AS (
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
    LATERAL VIEW OUTER EXPLODE(context) t AS key, value
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        status_code,
        key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS has_fault
    FROM flattened
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(status_code_high AS DOUBLE) AS status_code_high_num
    FROM parsed
    WHERE key IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(status_code * 0.9, 2) AS status_code_adjusted,
        has_fault - status_code_high AS has_fault_minus_status_code_high
    FROM filtered
    WHERE key IN ('trace', 'version')
),
totals AS (
    -- Multi-level aggregation
    SELECT
        key,
        CASE WHEN has_fault >= 1 THEN 'LARGE' ELSE 'SMALL' END AS has_fault_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE key = 'region') AS region_rows,
        COUNT(*) AS cnt,
        BIT_OR(status_code) AS status_code_bit_or,
        COUNT(*) FILTER (WHERE service = 'auth') AS auth_rows,
        GROUPING(key) AS gid
    FROM base
    GROUP BY ROLLUP (key, CASE WHEN has_fault >= 1 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    key,
    has_fault_tier,
    distinct_ids,
    region_rows,
    cnt,
    status_code_bit_or,
    gid
FROM totals
ORDER BY region_rows DESC NULLS FIRST;
