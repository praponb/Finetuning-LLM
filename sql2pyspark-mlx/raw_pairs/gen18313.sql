WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        attr_key,
        attr_value,
        MIN(latency_ms) OVER (PARTITION BY attr_key) AS attr_key_min_latency_ms,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY attr_key), 4) AS latency_ms_share,
        STDDEV_SAMP(latency_ms) OVER (PARTITION BY attr_key) AS attr_key_stddev_samp_latency_ms,
        MIN(latency_ms) OVER (PARTITION BY attr_key) AS attr_key_min_latency_ms_2,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY attr_key), 4) AS latency_ms_share_2
    FROM flattened
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(attr_key)) AS attr_key_title,
        INITCAP(TRIM(attr_key)) AS attr_key_title_2,
        COALESCE(attr_key, 'OTHER') AS attr_key_clean
    FROM metrics
    WHERE attr_key IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        attr_value,
        latency_ms_share,
        attr_key_stddev_samp_latency_ms,
        latency_ms_share_2,
        attr_key_title,
        PERCENT_RANK() OVER (ORDER BY latency_ms_share ASC) AS latency_ms_share_percent_rank,
        CUME_DIST() OVER (ORDER BY attr_key_min_latency_ms DESC NULLS LAST) AS attr_key_min_latency_ms_cume_dist
    FROM valid
),
with_windows_app_logs AS (
    -- Ranking and gap features
    SELECT
        attr_value,
        latency_ms_share,
        attr_key_title,
        latency_ms_share_percent_rank,
        DENSE_RANK() OVER (ORDER BY attr_key_stddev_samp_latency_ms ASC NULLS FIRST) AS attr_key_stddev_samp_latency_ms_dense_rank,
        PERCENT_RANK() OVER (ORDER BY latency_ms_share_percent_rank DESC) AS latency_ms_share_percent_rank_percent_rank,
        CUME_DIST() OVER (ORDER BY latency_ms_share DESC NULLS FIRST) AS latency_ms_share_cume_dist,
        DENSE_RANK() OVER (ORDER BY latency_ms_share_percent_rank ASC NULLS FIRST) AS latency_ms_share_percent_rank_dense_rank,
        PERCENT_RANK() OVER (ORDER BY latency_ms_share DESC) AS latency_ms_share_percent_rank_2
    FROM with_windows
)
-- Multi-level aggregation
SELECT
    CASE WHEN attr_key_stddev_samp_latency_ms_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS attr_key_stddev_samp_latency_ms_dense_rank_tier,
    CASE WHEN latency_ms_share_percent_rank >= 0.89 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_share_percent_rank_tier,
    COUNT(*) AS cnt,
    MIN(latency_ms_share_percent_rank_2) AS min_latency_ms_share_percent_rank_2,
    COUNT(*) AS row_count
FROM with_windows_app_logs
GROUP BY attr_key_stddev_samp_latency_ms_dense_rank_tier, latency_ms_share_percent_rank_tier
ORDER BY cnt ASC NULLS LAST
