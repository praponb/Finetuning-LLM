WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        v,
        RANK() OVER (PARTITION BY key ORDER BY status_code DESC) AS status_code_rank,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY level), 4) AS status_code_share_2
    FROM flattened
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        status_code_rank,
        status_code_share_2,
        CUME_DIST() OVER (ORDER BY status_code_share DESC) AS status_code_share_cume_dist,
        DENSE_RANK() OVER (ORDER BY status_code_share DESC) AS status_code_share_dense_rank,
        RANK() OVER (ORDER BY status_code_share_2 DESC NULLS FIRST) AS status_code_share_2_rank
    FROM metrics
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        status_code_rank,
        status_code_share_2,
        status_code_share_dense_rank,
        status_code_share_2_rank,
        status_code_share_dense_rank + status_code_share_cume_dist AS status_code_share_dense_rank_plus_status_code_share_cume_dist,
        status_code_rank / NULLIF(status_code_share_2, 0) AS status_code_rank_per_status_code_share_2
    FROM enriched
    WHERE status_code_share_cume_dist IS NOT NULL
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN status_code_share_2 >= 0.75 THEN 'LARGE' ELSE 'SMALL' END AS status_code_share_2_tier,
        CASE WHEN status_code_share_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_share_dense_rank_tier,
        AVG(status_code_share_2_rank) AS avg_status_code_share_2_rank,
        SUM(status_code_rank) AS total_status_code_rank,
        APPROX_PERCENTILE(status_code_share_2, ARRAY(0.25, 0.5, 0.75)) AS status_code_share_2_quartiles,
        COUNT(*) AS event_count
    FROM valid
    GROUP BY CASE WHEN status_code_share_2 >= 0.75 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN status_code_share_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    status_code_share_2_tier,
    status_code_share_dense_rank_tier,
    avg_status_code_share_2_rank,
    total_status_code_rank,
    status_code_share_2_quartiles
FROM grouped
ORDER BY total_status_code_rank NULLS FIRST
