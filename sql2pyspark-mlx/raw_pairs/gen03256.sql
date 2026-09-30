WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        LOWER(promo_code) AS promo_code_lower,
        DATE_ADD(ends_on, 13) AS ends_on_plus_13d,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM promotions
    WHERE channel IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on_plus_13d DESC, id) = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY MONTH(ends_on_plus_13d), promo_code), 4) AS discount_pct_share,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share_2,
        LEAD(discount_pct, 2) OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS next_discount_pct,
        NTILE(10) OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS discount_pct_bucket
    FROM ranked
),
ranked_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on_plus_13d DESC, id) AS rn
    FROM scored
    QUALIFY rn = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        discount_pct,
        promo_code_lower,
        ends_on_plus_13d,
        channel_clean,
        discount_pct_share_2,
        next_discount_pct,
        discount_pct_bucket,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
        NTILE(3) OVER (PARTITION BY MONTH(starts_on), promo_code ORDER BY discount_pct, id) AS discount_pct_bucket_2
    FROM ranked_promotions
),
metrics_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER w_part AS running_discount_pct_bucket,
        FIRST_VALUE(next_discount_pct, TRUE) OVER (ORDER BY discount_pct_share_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_discount_pct,
        DENSE_RANK() OVER (ORDER BY running_discount_pct) AS running_discount_pct_dense_rank
    FROM metrics
    WINDOW w_part AS (ORDER BY running_discount_pct, id)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(first_known_next_discount_pct, running_discount_pct_dense_rank) AS greatest_first_known_next_discount_pct_running_discount_pct_dense_rank
    FROM metrics_promotions
    WHERE discount_pct_share_2 > 0.37
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN discount_pct_bucket_2 >= 8 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_bucket_2_tier,
    DATE_TRUNC('QUARTER', ends_on_plus_13d) AS quarter_start,
    DATE_TRUNC('WEEK', ends_on_plus_13d) AS week_start,
    SUM(discount_pct_bucket) AS total_discount_pct_bucket,
    AVG(greatest_first_known_next_discount_pct_running_discount_pct_dense_rank) AS avg_greatest_first_known_next_discount_pct_running_discount_pct_dense_rank,
    SUM(running_discount_pct_dense_rank) AS total_running_discount_pct_dense_rank
FROM filtered
GROUP BY CUBE (CASE WHEN discount_pct_bucket_2 >= 8 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', ends_on_plus_13d), DATE_TRUNC('WEEK', ends_on_plus_13d))
HAVING AVG(greatest_first_known_next_discount_pct_running_discount_pct_dense_rank) > 0
ORDER BY week_start DESC
