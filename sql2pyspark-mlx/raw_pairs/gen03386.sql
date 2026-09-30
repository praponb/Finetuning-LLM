WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        service,
        host,
        level,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2
    FROM exploded
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(flag_bit2, 2) AS flag_bit2_rounded
    FROM flattened_attrs
    WHERE service IN ('auth', 'checkout', 'billing')
),
kept_keys AS (
    -- Set difference on keys
    SELECT host, service
    FROM filtered
    EXCEPT
    SELECT host, service
    FROM app_logs
    WHERE host = 'h2'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.host = k.host AND s.service = k.service
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY flag_bit2_rounded) AS flag_bit2_rounded_rank,
        DENSE_RANK() OVER (PARTITION BY level, host ORDER BY status_code_high) AS status_code_high_dense_rank
    FROM kept_rows
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        service,
        host,
        level,
        v,
        status_code_high,
        flag_bit2,
        flag_bit2_rounded,
        status_code_high_dense_rank,
        AVG(flag_bit2_rounded) OVER (PARTITION BY host) AS host_avg_flag_bit2_rounded
    FROM enriched
)
-- Multi-level aggregation
SELECT
    CASE WHEN flag_bit2_rounded >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END AS flag_bit2_rounded_tier,
    CASE WHEN host_avg_flag_bit2_rounded >= 83 THEN 'PREMIUM' ELSE 'STANDARD' END AS host_avg_flag_bit2_rounded_tier,
    level,
    COUNT(*) AS event_count,
    MIN(status_code_high) AS min_status_code_high,
    PERCENTILE_APPROX(status_code_high, ARRAY(0.25, 0.5, 0.75)) AS status_code_high_quartiles,
    COUNT(CASE WHEN host = 'h2' THEN 1 END) AS h2_count
FROM metrics
GROUP BY ROLLUP (CASE WHEN flag_bit2_rounded >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN host_avg_flag_bit2_rounded >= 83 THEN 'PREMIUM' ELSE 'STANDARD' END, level)
ORDER BY level DESC NULLS LAST
