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
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, v
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        host,
        latency_ms,
        k,
        v,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        host,
        latency_ms,
        k,
        v,
        flag_bit1,
        status_code_high,
        SUM(flag_bit1) OVER (PARTITION BY host, k) AS host_sum_flag_bit1,
        LEAD(flag_bit1, 1, 0) OVER (PARTITION BY host ORDER BY status_code_high, id, k) AS next_flag_bit1
    FROM flattened_attrs
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, status_code_high - host_sum_flag_bit1 AS status_code_high_minus_host_sum_flag_bit1
    FROM scored
    WHERE k IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY host, k ORDER BY status_code_high DESC NULLS LAST, id, k) = 1
)
SELECT
    id,
    service,
    host,
    k,
    flag_bit1,
    status_code_high,
    host_sum_flag_bit1,
    next_flag_bit1,
    status_code_high_minus_host_sum_flag_bit1,
    CASE WHEN host RLIKE '^(h4|h3)$' THEN 'GROUP_A' WHEN host RLIKE '^(h1|h2)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS host_zone
FROM ranked
ORDER BY id DESC NULLS LAST;
