SELECT
    id,
    ends_on,
    NTILE(4) OVER (PARTITION BY MONTH(ends_on), promo_code ORDER BY discount_pct DESC, id) AS discount_pct_bucket,
    LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
    LEAD(discount_pct, 1) OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS next_discount_pct
FROM promotions;
