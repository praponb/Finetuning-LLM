SELECT
    id,
    ends_on,
    discount_pct,
    NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', starts_on), promo_code ORDER BY discount_pct DESC, id) AS discount_pct_bucket,
    NTILE(5) OVER (PARTITION BY MONTH(ends_on), channel ORDER BY discount_pct DESC, id) AS discount_pct_bucket_2
FROM promotions
