WITH exploded AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, v
),
parsed AS (
    SELECT
        id,
        service,
        status_code,
        k,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged
    FROM exploded
),
ranked AS (
    SELECT *
    FROM parsed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code_high DESC, id, k) = 1
),
summary AS (
    SELECT
        CASE WHEN status_code >= 464 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
        k,
        COUNT(*) AS row_count,
        BIT_OR(status_code) AS status_code_bit_or,
        PERCENTILE_APPROX(status_code, 0.5) AS status_code_p50
    FROM ranked
    GROUP BY ROLLUP (CASE WHEN status_code >= 464 THEN 'PREMIUM' ELSE 'STANDARD' END, k)
)
SELECT
    status_code_tier,
    k,
    status_code_bit_or,
    status_code_p50
FROM summary
