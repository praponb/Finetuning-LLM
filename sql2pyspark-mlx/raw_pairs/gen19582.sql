SELECT
    id,
    shipping_fee,
    store_id,
    channel
FROM orders
WHERE shipping_fee <= 13;
