SELECT
    id,
    shipping_fee,
    discount,
    store_id
FROM orders
WHERE shipping_fee >= (SELECT MAX(shipping_fee) FROM orders)
ORDER BY id;
