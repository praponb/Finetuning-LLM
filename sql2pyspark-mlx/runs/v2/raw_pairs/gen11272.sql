SELECT
    id,
    order_date,
    customer_id,
    amount,
    ROW_NUMBER() OVER (ORDER BY order_date, id) AS order_date_row,
    MAX(amount) OVER (PARTITION BY customer_id) AS customer_id_max_amount
FROM orders
WHERE status = 'COMPLETED'
