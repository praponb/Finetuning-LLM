WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged
    FROM exploded
    WHERE attr_key IN ('region', 'trace', 'version')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        level,
        attr_key,
        attr_value,
        is_flagged,
        COUNT(is_flagged) OVER (PARTITION BY level, attr_key ORDER BY status_code_high, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_is_flagged,
        MIN(is_flagged) OVER (PARTITION BY level, attr_key ORDER BY status_code_high, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_is_flagged,
        SUM(is_flagged) OVER (PARTITION BY level, attr_key ORDER BY status_code_high, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_is_flagged,
        MAX(is_flagged) OVER (PARTITION BY level, attr_key ORDER BY status_code_high, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_is_flagged
    FROM flattened_attrs
    WHERE status_code > 436
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY running_max_is_flagged DESC NULLS FIRST, id, attr_key) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
)
-- Grouped metrics
SELECT
    level,
    COUNT(CASE WHEN level = 'DEBUG' THEN 1 END) AS debug_count,
    MIN(running_sum_is_flagged) AS min_running_sum_is_flagged
FROM ranked
GROUP BY level
ORDER BY min_running_sum_is_flagged DESC
