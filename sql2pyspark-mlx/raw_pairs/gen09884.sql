SELECT
    id,
    shipping_fee,
    customer_id,
    discount
FROM orders
WHERE shipping_fee <= (SELECT MIN(shipping_fee) FROM orders)
