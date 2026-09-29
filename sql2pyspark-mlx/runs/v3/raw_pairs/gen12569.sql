SELECT
    id,
    status,
    customer_id,
    order_date,
    amount,
    LAG(amount, 1) OVER (PARTITION BY status, customer_id ORDER BY order_date, id) AS prev_amount
FROM orders
