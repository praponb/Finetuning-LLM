SELECT
    id,
    shipping_fee,
    store_id,
    channel
FROM orders
WHERE shipping_fee >= (SELECT MAX(shipping_fee) FROM orders)
ORDER BY channel;
