SELECT
    id,
    amount,
    order_date,
    customer_id
FROM orders
WHERE amount >= (SELECT MAX(amount) FROM orders WHERE customer_id IN (35, 38, 40))
ORDER BY order_date, id
LIMIT 3
