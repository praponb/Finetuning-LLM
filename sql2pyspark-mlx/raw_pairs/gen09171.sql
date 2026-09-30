WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service, key ORDER BY latency_ms, id, key) AS row_num
        FROM exploded
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY latency_ms NULLS LAST) AS latency_ms_dense_rank,
        AVG(latency_ms) OVER (ORDER BY log_ts, id, key ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS latency_ms_avg_5,
        NTILE(5) OVER (PARTITION BY service, key ORDER BY latency_ms, id, key) AS latency_ms_bucket
    FROM latest
),
metrics AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        key,
        attr_value,
        latency_ms_dense_rank,
        latency_ms_bucket,
        COUNT(latency_ms) OVER (PARTITION BY key, service ORDER BY row_num, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_latency_ms,
        AVG(latency_ms) OVER (PARTITION BY key, service ORDER BY row_num, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_latency_ms,
        MAX(latency_ms) OVER (PARTITION BY key, service ORDER BY row_num, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_latency_ms,
        SUM(latency_ms) OVER (PARTITION BY key, service ORDER BY row_num, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_latency_ms,
        LAG(latency_ms_avg_5, 1) OVER (PARTITION BY key, service ORDER BY latency_ms_avg_5, id, key) AS prev_latency_ms_avg_5
    FROM with_windows
    WHERE latency_ms < 2293
),
metrics_app_logs AS (
    SELECT
        *,
        AVG(latency_ms) OVER (PARTITION BY key) AS key_avg_latency_ms,
        NTILE(10) OVER (ORDER BY running_avg_latency_ms DESC, id, key) AS running_avg_latency_ms_bucket,
        SUM(latency_ms_dense_rank) OVER (PARTITION BY TO_DATE(log_ts), key ORDER BY running_avg_latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_latency_ms_dense_rank
    FROM metrics
),
cleaned AS (
    SELECT *, GREATEST(running_count_latency_ms, running_avg_latency_ms) AS greatest_running_count_latency_ms_running_avg_latency_ms
    FROM metrics_app_logs
    WHERE log_ts >= TIMESTAMP '2025-06-01 07:00:00'
      AND latency_ms_bucket BETWEEN 3 AND 5
)
SELECT
    id,
    log_ts,
    key,
    latency_ms_dense_rank,
    latency_ms_bucket,
    running_avg_latency_ms,
    running_avg_latency_ms_bucket,
    AVG(latency_ms) OVER w_ordered AS running_avg_latency_ms_2,
    MAX(latency_ms) OVER w_ordered AS running_max_latency_ms_2,
    MIN(latency_ms) OVER w_ordered AS running_min_latency_ms
FROM cleaned
WINDOW w_ordered AS (PARTITION BY key ORDER BY running_avg_latency_ms_bucket, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
