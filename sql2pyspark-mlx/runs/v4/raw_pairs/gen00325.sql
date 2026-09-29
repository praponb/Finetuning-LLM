SELECT
    *,
    FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
    CUME_DIST() OVER (PARTITION BY channel ORDER BY discount_pct) AS discount_pct_cume_dist,
    DENSE_RANK() OVER w_part AS discount_pct_dense_rank
FROM promotions
WINDOW w_part AS (PARTITION BY channel ORDER BY discount_pct DESC)
