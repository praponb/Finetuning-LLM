WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        k,
        CAST((status_code & 8) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        has_fault - status_code_high has_fault_minus_status_code_high,
        COALESCE(k, 'DIRECT') k_filled
    FROM flattened_attrs
    WHERE host IN ('h2', 'h3')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY has_fault_minus_status_code_high NULLS FIRST) has_fault_minus_status_code_high_cume_dist,
        MAX(status_code_high) OVER (PARTITION BY host) host_max_status_code_high,
        RANK() OVER (PARTITION BY k ORDER BY has_fault_minus_status_code_high NULLS FIRST) has_fault_minus_status_code_high_rank,
        RANK() OVER (ORDER BY has_fault_minus_status_code_high DESC) has_fault_minus_status_code_high_rank_2,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY status_code_high NULLS LAST) status_code_high_dense_rank
    FROM valid
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        k,
        has_fault,
        status_code_high,
        has_fault_minus_status_code_high_cume_dist,
        has_fault_minus_status_code_high_rank,
        has_fault_minus_status_code_high_rank_2,
        status_code_high_dense_rank,
        TRIM(k) clean_k,
        COALESCE(k, 'DIRECT') resolved_k
    FROM enriched
    WHERE k = 'version'
),
base_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        status_code_high_dense_rank / NULLIF(status_code_high, 0) status_code_high_dense_rank_per_status_code_high,
        COALESCE(k, 'UNKNOWN') k_clean
    FROM base
    WHERE has_fault_minus_status_code_high_rank_2 >= 4
)
-- Aggregate with conditional metrics
SELECT
    host,
    APPROX_PERCENTILE(status_code_high_dense_rank_per_status_code_high, ARRAY(0.25, 0.5, 0.75)) AS status_code_high_dense_rank_per_status_code_high_quartiles,
    PERCENTILE_APPROX(has_fault, 0.95) AS has_fault_p95,
    ROUND(AVG(has_fault_minus_status_code_high_cume_dist), 2) AS avg_has_fault_minus_status_code_high_cume_dist,
    SORT_ARRAY(COLLECT_SET(host)) AS host_list,
    COUNT(*) AS event_count,
    SUM(status_code_high) FILTER (WHERE host = 'h3') AS h3_total_status_code_high
FROM base_app_logs
GROUP BY host
ORDER BY event_count NULLS FIRST;
