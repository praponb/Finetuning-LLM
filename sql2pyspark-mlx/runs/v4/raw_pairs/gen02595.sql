SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    SUM(discount_pct) OVER (PARTITION BY channel, promo_code) AS channel_sum_discount_pct,
    ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code, channel), 4) AS discount_pct_share,
    SUM(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct
FROM promotions;
