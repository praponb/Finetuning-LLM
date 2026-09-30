SELECT
    id,
    channel,
    shipping_fee,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee, id) AS rn
FROM orders
WHERE customer_id IN (8, 10);
