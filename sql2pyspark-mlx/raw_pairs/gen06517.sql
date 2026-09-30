WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, v
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY latency_ms, id, key) AS rank_in_group
    FROM lines
    QUALIFY rank_in_group = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY host, key ORDER BY rank_in_group DESC, id, key) AS seq_no,
        NTILE(3) OVER (ORDER BY latency_ms DESC, id, key) AS latency_ms_bucket
    FROM latest
    WHERE level IN ('ERROR', 'WARN')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        latency_ms,
        key,
        v,
        seq_no,
        latency_ms_bucket,
        COUNT(latency_ms_bucket) OVER (PARTITION BY key ORDER BY latency_ms_bucket, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_latency_ms_bucket,
        AVG(latency_ms_bucket) OVER (PARTITION BY key ORDER BY latency_ms_bucket, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_latency_ms_bucket,
        MAX(latency_ms_bucket) OVER (PARTITION BY key ORDER BY latency_ms_bucket, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_latency_ms_bucket
    FROM metrics
)
SELECT
    id,
    latency_ms,
    key,
    running_avg_latency_ms_bucket,
    running_max_latency_ms_bucket
FROM with_windows
ORDER BY running_max_latency_ms_bucket
