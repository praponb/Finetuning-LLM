SELECT
    id,
    customer_id,
    order_date,
    amount,
    status,
    discount,
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS running_amount,
    LAG(discount, 2) OVER (PARTITION BY status ORDER BY order_date, id) AS prev_discount
FROM orders
