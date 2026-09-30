WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        CONCAT(product, '#', CAST(holder_id AS STRING)) AS product_ref,
        CASE WHEN premium >= 2844.1 THEN 'large' WHEN premium >= 2096.6 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM policies
    WHERE holder_id IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY premium_tier) AS rows_in_premium_tier,
        MAX(premium) OVER w_premium_tier AS premium_tier_max_premium,
        COUNT(*) OVER (PARTITION BY holder_id ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY premium_tier), 0), 4) AS premium_share_of_premium_tier,
        ROUND(premium - AVG(premium) OVER w_premium_tier, 2) AS premium_vs_premium_tier_avg,
        SUM(premium) OVER (PARTITION BY premium_tier) AS premium_tier_sum_premium
    FROM prepared
    WINDOW w_premium_tier AS (PARTITION BY premium_tier)
),
status_stats AS (
    -- Median premium per status
    SELECT status, PERCENTILE_APPROX(premium, 0.5) AS median_premium
    FROM sequenced
    GROUP BY status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_premium,
        ROUND(s.premium - g.median_premium, 2) AS premium_minus_median
    FROM sequenced s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium >= g.median_premium
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        product_ref,
        premium_tier,
        rows_in_premium_tier,
        running_count,
        premium_share_of_premium_tier,
        premium_vs_premium_tier_avg,
        premium_tier_sum_premium,
        median_premium,
        premium_minus_median,
        ROUND(AVG(premium_tier_max_premium) OVER w_premium_tier, 2) AS premium_tier_avg_premium_tier_max_premium,
        MAX(rows_in_premium_tier) OVER (PARTITION BY premium_tier) AS premium_tier_max_rows_in_premium_tier
    FROM above_average
    WINDOW w_premium_tier AS (PARTITION BY premium_tier)
)
SELECT
    COALESCE(premium_tier, 'ALL') AS premium_tier,
    COALESCE(product, 'ALL') AS product,
    COALESCE(status, 'ALL') AS status,
    GROUPING_ID() AS grouping_level,
    COUNT(*) FILTER (WHERE status IN ('EXPIRED', 'ACTIVE', 'CANCELLED')) AS n_status_in_expired_active_can,
    COUNT(*) AS cnt,
    MIN(premium_tier_sum_premium) AS min_premium_tier_sum_premium
FROM windowed
GROUP BY ROLLUP(premium_tier, product, status)
ORDER BY premium_tier NULLS LAST, product NULLS LAST, status NULLS LAST
