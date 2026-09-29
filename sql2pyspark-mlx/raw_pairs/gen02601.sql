WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, attr_value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        level,
        status_code,
        attr_value,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY status_code DESC) AS status_code_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_percent_rank
    FROM unpacked
    WHERE status_code > 269
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY status_code_cume_dist NULLS FIRST) AS status_code_cume_dist_cume_dist,
        PERCENT_RANK() OVER w_grp AS status_code_cume_dist_percent_rank,
        DENSE_RANK() OVER (ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_dense_rank
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY level ORDER BY status_code_cume_dist DESC)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        level,
        status_code,
        attr_value,
        status_code_cume_dist,
        status_code_cume_dist_cume_dist,
        status_code_high_dense_rank,
        status_code_high_percent_rank + status_code_cume_dist AS status_code_high_percent_rank_plus_status_code_cume_dist
    FROM metrics
    WHERE status_code = 403
),
kept_keys AS (
    -- Set difference on keys
    SELECT level
    FROM base
    EXCEPT ALL
    SELECT level
    FROM app_logs
    WHERE service IN ('search', 'checkout', 'auth')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.level = k.level
)
-- Multi-level aggregation
SELECT
    level,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    BIT_OR(status_code) AS status_code_bit_or,
    COUNT(DISTINCT level) AS distinct_levels,
    ROUND(AVG(status_code), 2) AS avg_status_code
FROM kept_rows
GROUP BY level
ORDER BY distinct_levels
