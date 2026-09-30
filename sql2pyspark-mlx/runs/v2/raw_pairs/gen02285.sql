SELECT
    id,
    channel,
    discount,
    DENSE_RANK() OVER (PARTITION BY channel ORDER BY discount DESC) AS discount_dense_rank
FROM orders
WHERE store_id = 31
