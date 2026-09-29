SELECT
    payments.amount,
    payments.paid_at,
    payments.status,
    orders.order_date,
    orders.customer_id
FROM payments
LEFT JOIN orders ON payments.order_id = orders.id;
