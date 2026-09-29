SELECT
    id,
    channel,
    promo_code,
    starts_on,
    discount_pct,
    AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_2_prior,
    CUME_DIST() OVER (PARTITION BY channel ORDER BY discount_pct DESC) AS discount_pct_cume_dist,
    AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_2_prior_2
FROM promotions;
