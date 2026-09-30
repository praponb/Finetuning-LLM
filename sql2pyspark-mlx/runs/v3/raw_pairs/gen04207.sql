SELECT
    payments.id,
    payments.status,
    payments.amount,
    payments.paid_at,
    orders.status AS order_status
FROM payments
INNER JOIN orders ON payments.order_id = orders.id
WHERE orders.amount < 730 AND payments.status = 'FAILED';
