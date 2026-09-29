SELECT
    id,
    channel,
    starts_on,
    ends_on,
    discount_pct,
    ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY starts_on DESC, id) AS row_seq,
    SUM(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
    LAG(discount_pct, 1) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id) AS prev_discount_pct,
    ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share
FROM promotions
WHERE ends_on BETWEEN '2023-08-01' AND '2025-05-01';
