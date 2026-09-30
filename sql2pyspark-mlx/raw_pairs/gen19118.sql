WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, attr_value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        key,
        attr_value,
        AVG(status_code) OVER w_hist AS last7_avg_status_code,
        MAX(status_code) OVER w_hist AS last7_max_status_code,
        SUM(status_code) OVER w_hist AS last7_sum_status_code,
        SUM(status_code) OVER (PARTITION BY host, key) AS host_sum_status_code
    FROM exploded
    WINDOW w_hist AS (PARTITION BY key, level ORDER BY status_code, id, key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        key,
        attr_value,
        last7_avg_status_code,
        last7_max_status_code,
        host_sum_status_code,
        last7_max_status_code - host_sum_status_code AS last7_max_status_code_minus_host_sum_status_code,
        COALESCE(key, 'N/A') AS key_filled
    FROM metrics
    WHERE key IS NOT NULL
      AND last7_avg_status_code < 350
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        key,
        last7_avg_status_code,
        last7_max_status_code,
        key_filled,
        CUME_DIST() OVER (PARTITION BY key ORDER BY last7_avg_status_code) AS last7_avg_status_code_cume_dist,
        LAG(last7_max_status_code_minus_host_sum_status_code, 1) OVER (PARTITION BY key ORDER BY last7_avg_status_code, id, key) AS prev_last7_max_status_code_minus_host_sum_status_code
    FROM base
    WHERE last7_max_status_code_minus_host_sum_status_code IS NOT NULL
)
SELECT
    id,
    key,
    last7_avg_status_code,
    last7_max_status_code,
    key_filled,
    last7_avg_status_code_cume_dist
FROM with_windows;
