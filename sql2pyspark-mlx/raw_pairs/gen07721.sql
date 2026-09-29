WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, attr_value
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        level,
        status_code,
        key,
        attr_value,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
base AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(level) AS level_lower
    FROM unpacked
    WHERE key = 'trace'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        status_code,
        key,
        is_flagged,
        level_lower,
        SUM(is_flagged) OVER w_hist AS level_sum_is_flagged,
        SUM(is_flagged) OVER (PARTITION BY level, key ORDER BY is_flagged, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_flagged,
        AVG(is_flagged) OVER w_hist AS level_avg_is_flagged
    FROM base
    WINDOW w_hist AS (PARTITION BY level, key)
)
-- Multi-level aggregation
SELECT
    CASE WHEN level_avg_is_flagged >= 1 THEN 'LARGE' ELSE 'SMALL' END AS level_avg_is_flagged_tier,
    ROUND(AVG(level_avg_is_flagged), 2) AS avg_level_avg_is_flagged,
    SIZE(COLLECT_SET(key)) AS num_keys
FROM enriched
GROUP BY CASE WHEN level_avg_is_flagged >= 1 THEN 'LARGE' ELSE 'SMALL' END
HAVING ROUND(AVG(level_avg_is_flagged), 2) > 0
ORDER BY level_avg_is_flagged_tier DESC;
