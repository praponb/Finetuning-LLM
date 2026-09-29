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
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(latency_ms) OVER (PARTITION BY k) AS k_avg_latency_ms,
        AVG(latency_ms) OVER (PARTITION BY k, level) AS k_avg_latency_ms_2,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY TO_DATE(log_ts), service), 4) AS latency_ms_share
    FROM flattened
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        level,
        latency_ms,
        k,
        k_avg_latency_ms,
        k_avg_latency_ms_2,
        latency_ms_share,
        CONCAT(k, ' - ', level) AS k_level,
        k_avg_latency_ms_2 - status_code AS k_avg_latency_ms_2_minus_status_code,
        COALESCE(k, 'OTHER') AS resolved_k
    FROM metrics
    WHERE log_ts >= TIMESTAMP '2025-06-01 05:00:00'
      AND host IN ('h2', 'h1')
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        host,
        level,
        k,
        k_avg_latency_ms,
        k_avg_latency_ms_2,
        latency_ms_share,
        k_level,
        k_avg_latency_ms_2_minus_status_code,
        resolved_k,
        ROUND(k_avg_latency_ms_2_minus_status_code / k_avg_latency_ms_2, 2) AS k_avg_latency_ms_2_minus_status_code_per_k_avg_latency_ms_2,
        GREATEST(k_avg_latency_ms, latency_ms) AS greatest_k_avg_latency_ms_latency_ms
    FROM filtered
    WHERE k IS NOT DISTINCT FROM 'version'
      AND host IN ('h2', 'h3', 'h4')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        level,
        k,
        k_avg_latency_ms,
        latency_ms_share,
        k_level,
        k_avg_latency_ms_2_minus_status_code,
        resolved_k,
        k_avg_latency_ms_2_minus_status_code_per_k_avg_latency_ms_2,
        greatest_k_avg_latency_ms_latency_ms,
        AVG(latency_ms_share) OVER (PARTITION BY level) AS level_avg_latency_ms_share,
        CUME_DIST() OVER (ORDER BY k_avg_latency_ms DESC NULLS LAST) AS k_avg_latency_ms_cume_dist,
        MIN(k_avg_latency_ms_2_minus_status_code) OVER (PARTITION BY k) AS k_min_k_avg_latency_ms_2_minus_status_code
    FROM valid
    WHERE k_avg_latency_ms_2_minus_status_code_per_k_avg_latency_ms_2 >= 79
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT level
    FROM with_windows
    INTERSECT ALL
    SELECT level
    FROM app_logs
    WHERE service = 'checkout'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN remaining_keys k ON s.level = k.level
),
remaining_keys_2 AS (
    -- Set difference on keys
    SELECT level
    FROM kept_rows
    EXCEPT ALL
    SELECT level
    FROM app_logs
    WHERE service NOT IN ('search', 'checkout')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    JOIN remaining_keys_2 k ON s.level = k.level
)
SELECT
    level,
    k,
    latency_ms_share,
    k_avg_latency_ms_2_minus_status_code,
    resolved_k,
    k_avg_latency_ms_2_minus_status_code_per_k_avg_latency_ms_2,
    level_avg_latency_ms_share,
    k_avg_latency_ms_cume_dist,
    k_min_k_avg_latency_ms_2_minus_status_code
FROM verified
ORDER BY k_avg_latency_ms_2_minus_status_code DESC;
