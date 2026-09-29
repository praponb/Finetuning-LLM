SELECT
    id,
    amount,
    order_date,
    status
FROM orders
WHERE amount >= (SELECT MAX(amount) FROM orders);
