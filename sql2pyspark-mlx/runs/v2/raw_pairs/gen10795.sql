SELECT
    payments.method,
    payments.status,
    payments.paid_at,
    orders.amount,
    orders.store_id
FROM payments
FULL OUTER JOIN orders ON payments.order_id = orders.id;
