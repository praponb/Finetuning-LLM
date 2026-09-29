SELECT
    *,
    FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
    CUME_DIST() OVER w_ordered AS discount_pct_cume_dist,
    LAG(discount_pct, 2) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id) AS prev_discount_pct
FROM promotions
WHERE channel IS NOT NULL
WINDOW w_ordered AS (PARTITION BY promo_code ORDER BY discount_pct);
