WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, value
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        host,
        level,
        latency_ms,
        status_code,
        value,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
kept_keys AS (
    -- Set intersection on keys
    SELECT level, host
    FROM unpacked
    INTERSECT ALL
    SELECT level, host
    FROM app_logs
    WHERE level != 'INFO'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM unpacked s
    INNER JOIN kept_keys k ON s.level = k.level AND s.host = k.host
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(status_code_high) OVER (PARTITION BY host) AS host_min_status_code_high,
        AVG(flag_bit1) OVER w_grp AS level_avg_flag_bit1,
        CUME_DIST() OVER (ORDER BY status_code_high DESC) AS status_code_high_cume_dist
    FROM reconciled
    WINDOW w_grp AS (PARTITION BY level)
),
enriched_app_logs AS (
    -- Running and moving aggregates
    SELECT
        host,
        level,
        value,
        flag_bit1,
        level_avg_flag_bit1,
        status_code_high_cume_dist,
        MAX(status_code_high) OVER (PARTITION BY host) AS host_max_status_code_high,
        MAX(level_avg_flag_bit1) OVER (PARTITION BY level) AS level_max_level_avg_flag_bit1,
        CUME_DIST() OVER (PARTITION BY level ORDER BY flag_bit1) AS flag_bit1_cume_dist
    FROM enriched
)
-- Multi-level aggregation
SELECT
    level,
    CASE WHEN status_code_high_cume_dist >= 0.87 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_cume_dist_tier,
    SUM(CASE WHEN host = 'h2' THEN level_avg_flag_bit1 ELSE 0 END) AS h2_level_avg_flag_bit1,
    COUNT(CASE WHEN level = 'WARN' THEN 1 END) AS warn_count,
    PERCENTILE_APPROX(flag_bit1_cume_dist, 0.95) AS flag_bit1_cume_dist_p95,
    APPROX_PERCENTILE(flag_bit1, 0.9) AS flag_bit1_p90
FROM enriched_app_logs
GROUP BY level, CASE WHEN status_code_high_cume_dist >= 0.87 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY flag_bit1_p90 DESC;
