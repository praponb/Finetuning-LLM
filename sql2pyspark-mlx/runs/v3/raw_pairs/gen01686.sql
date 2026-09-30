SELECT
    payments.id,
    payments.method,
    payments.status,
    payments.paid_at
FROM payments
WHERE payments.order_id IN (SELECT id FROM orders WHERE amount BETWEEN 440 AND 1170);
