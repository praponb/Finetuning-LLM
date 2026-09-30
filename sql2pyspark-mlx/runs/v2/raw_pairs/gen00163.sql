SELECT
    id,
    channel,
    discount,
    PERCENT_RANK() OVER (PARTITION BY channel ORDER BY discount DESC) discount_pct_rank
FROM orders
WHERE channel IN ('mobile', 'store')
ORDER BY discount;
