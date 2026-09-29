WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, v
),
flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        level,
        status_code,
        attr_key,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2
    FROM lines
),
kept_keys AS (
    -- Set difference on keys
    SELECT level
    FROM flattened_attrs
    EXCEPT DISTINCT
    SELECT level
    FROM app_logs
    WHERE latency_ms != 2055
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened_attrs s
    INNER JOIN kept_keys k ON s.level = k.level
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY status_code ASC) AS status_code_percent_rank,
        CUME_DIST() OVER (ORDER BY flag_bit2 DESC) AS flag_bit2_cume_dist
    FROM verified
),
scored_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share,
        MAX(status_code_percent_rank) OVER (PARTITION BY level) AS level_max_status_code_percent_rank,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY status_code_percent_rank DESC NULLS FIRST) AS status_code_percent_rank_cume_dist,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY flag_bit2_cume_dist ASC) AS flag_bit2_cume_dist_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY flag_bit2 ASC NULLS LAST) AS flag_bit2_percent_rank
    FROM scored
)
SELECT
    level,
    status_code,
    attr_key,
    v,
    status_code_high,
    flag_bit2,
    flag_bit2_cume_dist,
    status_code_share,
    status_code_percent_rank_cume_dist
FROM scored_app_logs
ORDER BY status_code_percent_rank_cume_dist DESC, level ASC
