SELECT
    id,
    starts_on,
    ends_on,
    discount_pct,
    NTILE(10) OVER w_part AS discount_pct_bucket,
    NTILE(5) OVER w_hist AS discount_pct_bucket_2,
    MIN(discount_pct) OVER w_grp AS channel_min_discount_pct,
    ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share
FROM promotions
WINDOW w_part AS (PARTITION BY channel ORDER BY discount_pct, id), w_hist AS (PARTITION BY promo_code ORDER BY discount_pct DESC, id), w_grp AS (PARTITION BY channel);
