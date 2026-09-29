SELECT
    id,
    shipping_fee,
    customer_id,
    channel
FROM orders
WHERE shipping_fee >= 12
