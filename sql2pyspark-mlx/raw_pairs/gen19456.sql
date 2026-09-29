WITH lines AS (
    SELECT
        id,
        host,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
parsed AS (
    SELECT
        id,
        status_code,
        key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS has_fault
    FROM lines
),
with_windows AS (
    SELECT
        status_code,
        key,
        CUME_DIST() OVER (PARTITION BY key ORDER BY has_fault NULLS FIRST) AS has_fault_cume_dist,
        PERCENT_RANK() OVER (ORDER BY has_fault) AS has_fault_percent_rank,
        PERCENT_RANK() OVER (ORDER BY status_code DESC) AS status_code_percent_rank
    FROM parsed
),
metrics AS (
    SELECT
        status_code,
        key,
        MIN(status_code_percent_rank) OVER (PARTITION BY key) AS key_min_status_code_percent_rank,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY has_fault_cume_dist) AS has_fault_cume_dist_dense_rank
    FROM with_windows
    WHERE key IN ('trace', 'region')
)
SELECT
    status_code,
    key,
    key_min_status_code_percent_rank,
    has_fault_cume_dist_dense_rank
FROM metrics
ORDER BY has_fault_cume_dist_dense_rank DESC NULLS FIRST, key;
