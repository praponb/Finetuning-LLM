WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT channel, promo_code
    FROM promotions
    INTERSECT
    SELECT channel, promo_code
    FROM promotions
    WHERE channel IN ('partner', 'mobile')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    INNER JOIN remaining_keys k ON s.channel = k.channel AND s.promo_code = k.promo_code
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY MONTH(starts_on), promo_code ORDER BY discount_pct) AS discount_pct_cume_dist,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share,
        COUNT(discount_pct) OVER (PARTITION BY channel) AS channel_count_discount_pct,
        DENSE_RANK() OVER w_part AS discount_pct_dense_rank
    FROM reconciled
    WHERE ends_on BETWEEN '2023-09-01' AND '2024-12-01'
    WINDOW w_part AS (ORDER BY discount_pct DESC)
),
with_windows_promotions AS (
    -- Ranking and gap features
    SELECT
        id,
        discount_pct_cume_dist,
        discount_pct_share,
        channel_count_discount_pct,
        discount_pct_dense_rank,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('month', starts_on), channel ORDER BY discount_pct_share NULLS LAST) AS discount_pct_share_dense_rank,
        DENSE_RANK() OVER w_part AS discount_pct_dense_rank_dense_rank,
        COUNT(discount_pct) OVER (PARTITION BY promo_code, channel) AS promo_code_count_discount_pct,
        DENSE_RANK() OVER (ORDER BY discount_pct) AS discount_pct_dense_rank_2,
        RANK() OVER w_part_2 AS discount_pct_rank
    FROM with_windows
    WINDOW w_part AS (PARTITION BY promo_code ORDER BY discount_pct_dense_rank DESC NULLS FIRST), w_part_2 AS (ORDER BY discount_pct DESC)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        discount_pct_share,
        discount_pct_share_dense_rank,
        promo_code_count_discount_pct,
        discount_pct_dense_rank_2,
        DENSE_RANK() OVER (ORDER BY channel_count_discount_pct DESC) AS channel_count_discount_pct_dense_rank,
        RANK() OVER (ORDER BY discount_pct_dense_rank NULLS LAST) AS discount_pct_dense_rank_rank,
        DENSE_RANK() OVER (ORDER BY channel_count_discount_pct) AS channel_count_discount_pct_dense_rank_2
    FROM with_windows_promotions
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(channel_count_discount_pct_dense_rank_2 * 1.15, 2) AS channel_count_discount_pct_dense_rank_2_adjusted,
        CASE WHEN discount_pct_share_dense_rank >= 8 THEN 'High' WHEN discount_pct_share_dense_rank >= 3 THEN 'Medium' ELSE 'Low' END AS discount_pct_share_dense_rank_band
    FROM metrics
    WHERE discount_pct_share > 0.61
),
with_windows_promotions_2 AS (
    -- Ranking and gap features
    SELECT
        discount_pct_share,
        promo_code_count_discount_pct,
        discount_pct_dense_rank_2,
        RANK() OVER (ORDER BY promo_code_count_discount_pct NULLS FIRST) AS promo_code_count_discount_pct_rank,
        DENSE_RANK() OVER (ORDER BY channel_count_discount_pct_dense_rank_2 NULLS LAST) AS channel_count_discount_pct_dense_rank_2_dense_rank,
        PERCENT_RANK() OVER (ORDER BY discount_pct_dense_rank_2 DESC NULLS LAST) AS discount_pct_dense_rank_2_percent_rank,
        PERCENT_RANK() OVER (ORDER BY discount_pct_dense_rank_rank DESC NULLS FIRST) AS discount_pct_dense_rank_rank_percent_rank
    FROM filtered
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN discount_pct_dense_rank_2_percent_rank >= 0.78 THEN 'LARGE' ELSE 'SMALL' END AS discount_pct_dense_rank_2_percent_rank_tier,
        CASE WHEN discount_pct_dense_rank_rank_percent_rank >= 0.75 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_dense_rank_rank_percent_rank_tier,
        ROUND(AVG(discount_pct_share), 2) AS avg_discount_pct_share,
        APPROX_PERCENTILE(discount_pct_dense_rank_2_percent_rank, 0.95) AS discount_pct_dense_rank_2_percent_rank_p95,
        GROUPING(CASE WHEN discount_pct_dense_rank_2_percent_rank >= 0.78 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_id
    FROM with_windows_promotions_2
    GROUP BY GROUPING SETS ((CASE WHEN discount_pct_dense_rank_2_percent_rank >= 0.78 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN discount_pct_dense_rank_rank_percent_rank >= 0.75 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN discount_pct_dense_rank_2_percent_rank >= 0.78 THEN 'LARGE' ELSE 'SMALL' END), ())
)
SELECT
    discount_pct_dense_rank_rank_percent_rank_tier,
    avg_discount_pct_share,
    discount_pct_dense_rank_2_percent_rank_p95
FROM stats
ORDER BY avg_discount_pct_share NULLS LAST;
