SELECT
    id,
    amount,
    order_date,
    shipping_fee
FROM orders
WHERE amount >= (SELECT MAX(amount) FROM orders);
