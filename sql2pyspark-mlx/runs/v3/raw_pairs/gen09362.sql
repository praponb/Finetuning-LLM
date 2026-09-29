SELECT
    id,
    status,
    order_date,
    amount,
    SUM(amount) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
FROM orders
