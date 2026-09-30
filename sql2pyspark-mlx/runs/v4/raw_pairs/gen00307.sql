WITH valid AS (
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        UPPER(promo_code) AS promo_code_upper,
        SUBSTRING(promo_code, 1, 2) AS promo_code_prefix
    FROM promotions
    WHERE promo_code IN ('SPRING10', 'WELCOME5', 'BUNDLE')
),
with_windows AS (
    SELECT
        *,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share,
        CUME_DIST() OVER (ORDER BY discount_pct DESC) AS discount_pct_cume_dist,
        NTILE(4) OVER (ORDER BY discount_pct, id) AS discount_pct_bucket
    FROM valid
),
base AS (
    SELECT
        id,
        channel,
        promo_code,
        discount_pct,
        promo_code_prefix,
        discount_pct_share,
        discount_pct_cume_dist,
        discount_pct_bucket,
        CASE WHEN channel IN ('mobile', 'store') THEN 'Priority' ELSE 'Other' END AS channel_group,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM with_windows
    WHERE ends_on >= DATE '2024-01-12'
      AND discount_pct BETWEEN 0.13 AND 0.28
),
totals AS (
    SELECT
        CASE WHEN discount_pct_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_bucket_tier,
        ROUND(SUM(discount_pct_bucket) / NULLIF(COUNT(CASE WHEN promo_code = 'WELCOME5' THEN 1 END), 0), 2) AS discount_pct_bucket_per_welcome5,
        MIN(discount_pct) AS min_discount_pct,
        SUM(CASE WHEN promo_code = 'VIP' THEN discount_pct_cume_dist ELSE 0 END) AS vip_discount_pct_cume_dist,
        SUM(CASE WHEN channel = 'store' THEN discount_pct_share ELSE 0 END) AS store_discount_pct_share
    FROM base
    GROUP BY discount_pct_bucket_tier
    HAVING vip_discount_pct_cume_dist > 0
)
SELECT
    discount_pct_bucket_tier,
    min_discount_pct,
    store_discount_pct_share
FROM totals
ORDER BY store_discount_pct_share DESC NULLS FIRST, discount_pct_bucket_tier;
