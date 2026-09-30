SELECT
    id,
    order_date,
    amount,
    shipping_fee
FROM orders
WHERE order_date >= (SELECT MAX(order_date) FROM orders)
