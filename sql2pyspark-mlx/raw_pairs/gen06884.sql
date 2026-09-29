SELECT
    id,
    starts_on,
    discount_pct,
    NTILE(5) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id) AS discount_pct_bucket,
    LAG(discount_pct, 1) OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS prev_discount_pct,
    SUM(discount_pct) OVER (PARTITION BY DATE_TRUNC('YEAR', starts_on), promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
    PERCENT_RANK() OVER (ORDER BY discount_pct) AS discount_pct_percent_rank,
    NTILE(5) OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id) AS discount_pct_bucket_2
FROM promotions
WHERE channel IS NULL
