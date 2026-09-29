WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
kept_keys AS (
    -- Set intersection on keys
    SELECT level
    FROM exploded
    INTERSECT ALL
    SELECT level
    FROM app_logs
    WHERE host = 'h3'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    JOIN kept_keys k ON s.level = k.level
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        key,
        SUM(latency_ms) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_sum_2h
    FROM reconciled
    WHERE level IN ('WARN', 'ERROR')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        log_ts,
        DENSE_RANK() OVER (ORDER BY latency_ms_sum_2h ASC NULLS FIRST) AS latency_ms_sum_2h_dense_rank,
        COUNT(latency_ms_sum_2h) OVER w_grp AS latency_ms_sum_2h_count_2h
    FROM metrics
    WINDOW w_grp AS (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(latency_ms_sum_2h_dense_rank, 2) AS latency_ms_sum_2h_dense_rank_rounded
    FROM enriched
    WHERE log_ts >= TIMESTAMP '2025-06-01 05:00:00'
),
valid_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, latency_ms_sum_2h_count_2h / NULLIF(latency_ms_sum_2h_dense_rank_rounded, 0) AS latency_ms_sum_2h_count_2h_per_latency_ms_sum_2h_dense_rank_rounded
    FROM valid
    WHERE log_ts > DATE '2025-06-01'
      AND latency_ms_sum_2h_dense_rank_rounded <= 29
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('HOUR', log_ts) AS hour_window,
        CASE WHEN latency_ms_sum_2h_count_2h >= 15 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_sum_2h_count_2h_tier,
        (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
        COUNT(*) AS num_rows,
        MIN(log_ts) AS started_at,
        COUNT(*) AS event_count
    FROM valid_app_logs
    GROUP BY hour_window, latency_ms_sum_2h_count_2h_tier
)
SELECT
    hour_window,
    latency_ms_sum_2h_count_2h_tier,
    num_rows,
    started_at,
    event_count
FROM summary
ORDER BY hour_window DESC, num_rows ASC;
