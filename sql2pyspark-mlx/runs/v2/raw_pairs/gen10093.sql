SELECT
    id,
    amount,
    order_date,
    channel
FROM orders
WHERE amount < (SELECT MIN(amount) FROM orders)
ORDER BY order_date
