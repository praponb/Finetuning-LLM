WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        latency_ms,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, v
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY attr_key ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_dense_rank,
        COUNT(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_count_15m
    FROM lines
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        log_ts,
        latency_ms,
        attr_key,
        v,
        latency_ms_dense_rank,
        latency_ms_count_15m,
        SUM(latency_ms) OVER (PARTITION BY attr_key) AS attr_key_sum_latency_ms,
        DENSE_RANK() OVER (ORDER BY latency_ms_count_15m DESC) AS latency_ms_count_15m_dense_rank,
        SUM(latency_ms_dense_rank) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_dense_rank_sum_15m
    FROM metrics
),
enriched_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(latency_ms_dense_rank_sum_15m) OVER w_hist AS attr_key_max_latency_ms_dense_rank_sum_15m,
        MAX(latency_ms) OVER w_hist AS attr_key_max_latency_ms
    FROM enriched
    WINDOW w_hist AS (PARTITION BY attr_key)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(latency_ms_dense_rank_sum_15m, 0) AS latency_ms_dense_rank_sum_15m_filled,
        CAST(attr_key_max_latency_ms AS STRING) AS attr_key_max_latency_ms_str
    FROM enriched_app_logs
    WHERE log_ts > TIMESTAMP '2025-06-01 09:00:00'
      AND attr_key_sum_latency_ms = 3066
)
-- Grouped metrics
SELECT
    CASE WHEN latency_ms_count_15m >= 16 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_count_15m_tier,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys,
    COUNT(*) AS num_rows,
    SUM(CASE WHEN attr_key = 'region' THEN latency_ms_dense_rank_sum_15m ELSE 0 END) AS region_latency_ms_dense_rank_sum_15m,
    COUNT(CASE WHEN attr_key = 'version' THEN 1 END) AS version_count,
    MAX(log_ts) AS last_seen,
    MAX(attr_key_max_latency_ms_dense_rank_sum_15m) FILTER (WHERE attr_key = 'region') AS region_max_attr_key_max_latency_ms_dense_rank_sum_15m
FROM valid
GROUP BY CASE WHEN latency_ms_count_15m >= 16 THEN 'HIGH' ELSE 'LOW' END;
