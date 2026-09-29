SELECT payments.id, payments.method, payments.paid_at
FROM payments
WHERE payments.order_id NOT IN (SELECT id FROM orders WHERE status IN ('RETURNED', 'COMPLETED'))
ORDER BY id DESC;
