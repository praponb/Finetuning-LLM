SELECT
    id,
    amount,
    customer_id,
    order_date
FROM orders
WHERE amount = (SELECT MIN(amount) FROM orders)
ORDER BY order_date;
