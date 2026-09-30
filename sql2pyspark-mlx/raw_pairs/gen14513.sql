WITH base AS (
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        CASE WHEN channel IN ('store', 'web') THEN 'Priority' ELSE 'Other' END AS channel_group,
        LENGTH(channel) AS channel_length,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM promotions
    WHERE promo_code NOT IN ('WELCOME5', 'BUNDLE', 'SPRING10')
      AND starts_on > DATE '2023-09-28'
),
with_windows AS (
    SELECT
        id,
        promo_code,
        ends_on,
        channel_group,
        channel_length,
        channel_clean,
        NTILE(10) OVER w_hist AS discount_pct_bucket,
        NTILE(3) OVER w_part AS discount_pct_bucket_2
    FROM base
    WHERE promo_code = 'VIP'
    WINDOW w_hist AS (PARTITION BY YEAR(ends_on), promo_code ORDER BY discount_pct, id), w_part AS (PARTITION BY promo_code ORDER BY discount_pct, id)
),
base_promotions AS (
    SELECT *, channel_length + discount_pct_bucket_2 AS channel_length_plus_discount_pct_bucket_2
    FROM with_windows
    WHERE ends_on > DATE '2023-09-09'
      AND ends_on > '2025-05-20'
),
filtered AS (
    SELECT
        id,
        promo_code,
        ends_on,
        channel_group,
        channel_clean,
        discount_pct_bucket,
        discount_pct_bucket_2,
        CAST(discount_pct_bucket_2 AS DOUBLE) AS discount_pct_bucket_2_num,
        DATEDIFF(CURRENT_DATE(), ends_on) AS days_since_ends_on
    FROM base_promotions
    WHERE discount_pct_bucket BETWEEN 6 AND 7
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct_bucket_2_num DESC) AS discount_pct_bucket_2_num_dense_rank,
        AVG(discount_pct_bucket) OVER (PARTITION BY promo_code) AS promo_code_avg_discount_pct_bucket
    FROM filtered
),
grouped AS (
    SELECT
        CASE WHEN discount_pct_bucket_2_num_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS discount_pct_bucket_2_num_dense_rank_tier,
        MAX(discount_pct_bucket_2_num_dense_rank) AS max_discount_pct_bucket_2_num_dense_rank,
        COUNT_IF(promo_code = 'WELCOME5') AS welcome5_events,
        PERCENTILE_APPROX(discount_pct_bucket_2_num, 0.95) AS discount_pct_bucket_2_num_p95
    FROM enriched
    GROUP BY discount_pct_bucket_2_num_dense_rank_tier
)
SELECT discount_pct_bucket_2_num_dense_rank_tier, discount_pct_bucket_2_num_p95
FROM grouped
ORDER BY discount_pct_bucket_2_num_dense_rank_tier DESC, discount_pct_bucket_2_num_p95
