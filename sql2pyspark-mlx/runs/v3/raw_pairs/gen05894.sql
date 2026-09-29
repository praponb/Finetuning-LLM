SELECT
    id,
    customer_id,
    order_date,
    status,
    amount,
    NTILE(3) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS order_date_ntile,
    LEAD(amount, 1) OVER (PARTITION BY status, customer_id ORDER BY order_date, id) AS next_amount
FROM orders
ORDER BY status
