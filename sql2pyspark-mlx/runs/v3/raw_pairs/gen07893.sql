SELECT
    id,
    shipping_fee,
    channel,
    store_id
FROM orders
WHERE shipping_fee > (SELECT MAX(shipping_fee) FROM orders)
ORDER BY channel, store_id DESC;
