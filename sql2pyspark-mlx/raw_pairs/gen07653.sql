WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY status_code) AS status_code_cume_dist,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share
    FROM lines
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        attr_key,
        attr_value,
        status_code_cume_dist,
        LENGTH(attr_key) AS attr_key_length
    FROM with_windows
    WHERE status_code_cume_dist BETWEEN 0.3 AND 0.75
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        attr_value,
        SUM(status_code_cume_dist) OVER (PARTITION BY attr_key) AS attr_key_sum_status_code_cume_dist,
        CUME_DIST() OVER (ORDER BY status_code_cume_dist NULLS FIRST) AS status_code_cume_dist_cume_dist,
        RANK() OVER (PARTITION BY attr_key ORDER BY status_code_cume_dist) AS status_code_cume_dist_rank,
        DENSE_RANK() OVER (ORDER BY attr_key_length NULLS LAST) AS attr_key_length_dense_rank
    FROM valid
    WHERE attr_key IN ('version', 'trace')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(status_code_cume_dist_rank * 0.9, 2) AS adjusted_status_code_cume_dist_rank,
        ROUND(attr_key_sum_status_code_cume_dist, 0) AS attr_key_sum_status_code_cume_dist_rounded
    FROM metrics
    WHERE status_code_cume_dist_rank < 5
)
SELECT
    attr_value,
    attr_key_sum_status_code_cume_dist,
    status_code_cume_dist_rank,
    adjusted_status_code_cume_dist_rank,
    attr_key_sum_status_code_cume_dist_rounded
FROM base
ORDER BY status_code_cume_dist_rank DESC NULLS FIRST;
