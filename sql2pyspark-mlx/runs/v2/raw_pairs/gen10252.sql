SELECT
    payments.id,
    payments.method,
    payments.amount,
    orders.order_date,
    orders.discount,
    orders.status,
    DATE_TRUNC('month', orders.order_date) AS order_date_period
FROM payments
JOIN orders ON payments.order_id = orders.id
WHERE YEAR(orders.order_date) = 2023 AND orders.amount BETWEEN 420 AND 810;
