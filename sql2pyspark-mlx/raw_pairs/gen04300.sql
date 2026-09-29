SELECT
    *,
    LAG(discount_pct, 1) OVER w_recent prev_discount_pct,
    LAG(discount_pct, 1) OVER (PARTITION BY channel ORDER BY ends_on, id) prev_discount_pct_2
FROM promotions
WHERE channel = 'mobile'
WINDOW w_recent AS (PARTITION BY promo_code ORDER BY discount_pct, id)
