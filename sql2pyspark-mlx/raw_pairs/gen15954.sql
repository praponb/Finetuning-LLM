WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, attr_value
),
flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT CAST((status_code & 4) != 0 AS INT) AS flag_bit4, SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
enriched AS (
    -- Ranking and gap features
    SELECT
        flag_bit4,
        status_code_high,
        CUME_DIST() OVER (ORDER BY flag_bit4 DESC NULLS LAST) AS flag_bit4_cume_dist
    FROM flattened_attrs
),
scored AS (
    -- Ranking and gap features
    SELECT
        status_code_high,
        flag_bit4_cume_dist,
        RANK() OVER (ORDER BY flag_bit4 NULLS LAST) AS flag_bit4_rank,
        CUME_DIST() OVER (ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_cume_dist
    FROM enriched
)
-- Multi-level aggregation
SELECT
    CASE WHEN flag_bit4_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS flag_bit4_rank_tier,
    SUM(flag_bit4_rank) AS total_flag_bit4_rank,
    PERCENTILE_APPROX(flag_bit4_rank, ARRAY(0.25, 0.5, 0.75)) AS flag_bit4_rank_quartiles,
    APPROX_PERCENTILE(status_code_high, ARRAY(0.25, 0.5, 0.75)) AS status_code_high_quartiles,
    ROUND(AVG(flag_bit4_rank), 2) AS avg_flag_bit4_rank,
    PERCENTILE_APPROX(status_code_high, 0.95) AS status_code_high_p95
FROM scored
GROUP BY 1
ORDER BY status_code_high_p95;
