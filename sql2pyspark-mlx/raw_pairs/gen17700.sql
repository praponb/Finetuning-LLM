WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        log_ts,
        latency_ms,
        k,
        AVG(latency_ms) OVER (PARTITION BY k) AS k_avg_latency_ms,
        PERCENT_RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_percent_rank,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_dense_rank,
        DENSE_RANK() OVER w_grp AS status_code_dense_rank
    FROM flattened
    WINDOW w_grp AS (PARTITION BY YEAR(log_ts) ORDER BY status_code)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        k_avg_latency_ms,
        latency_ms_percent_rank,
        COUNT(latency_ms_percent_rank) OVER (PARTITION BY MONTH(log_ts), k) AS log_ts_month_count_latency_ms_percent_rank,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY latency_ms_percent_rank DESC NULLS LAST) AS latency_ms_percent_rank_dense_rank
    FROM metrics
    WHERE log_ts >= '2024-06-01'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(latency_ms_percent_rank, 0) AS latency_ms_percent_rank_filled,
        ROUND(log_ts_month_count_latency_ms_percent_rank, 2) AS log_ts_month_count_latency_ms_percent_rank_rounded
    FROM scored
    WHERE latency_ms_percent_rank_dense_rank = 6
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        latency_ms_percent_rank + latency_ms_percent_rank_dense_rank AS latency_ms_percent_rank_plus_latency_ms_percent_rank_dense_rank,
        latency_ms_percent_rank_dense_rank - latency_ms_percent_rank AS latency_ms_percent_rank_dense_rank_minus_latency_ms_percent_rank
    FROM valid
    WHERE log_ts_month_count_latency_ms_percent_rank <> 9
)
-- Grouped metrics
SELECT
    CASE WHEN log_ts_month_count_latency_ms_percent_rank >= 18 THEN 'HIGH' ELSE 'LOW' END AS log_ts_month_count_latency_ms_percent_rank_tier,
    CASE WHEN latency_ms_percent_rank_dense_rank_minus_latency_ms_percent_rank >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_percent_rank_dense_rank_minus_latency_ms_percent_rank_tier,
    CASE WHEN latency_ms_percent_rank_plus_latency_ms_percent_rank_dense_rank >= 73 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_percent_rank_plus_latency_ms_percent_rank_dense_rank_tier,
    PERCENTILE_APPROX(latency_ms_percent_rank_plus_latency_ms_percent_rank_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_percent_rank_plus_latency_ms_percent_rank_dense_rank_quartiles,
    PERCENTILE_APPROX(latency_ms_percent_rank_dense_rank_minus_latency_ms_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_percent_rank_dense_rank_minus_latency_ms_percent_rank_quartiles
FROM filtered
GROUP BY CASE WHEN log_ts_month_count_latency_ms_percent_rank >= 18 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN latency_ms_percent_rank_dense_rank_minus_latency_ms_percent_rank >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN latency_ms_percent_rank_plus_latency_ms_percent_rank_dense_rank >= 73 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY log_ts_month_count_latency_ms_percent_rank_tier;
