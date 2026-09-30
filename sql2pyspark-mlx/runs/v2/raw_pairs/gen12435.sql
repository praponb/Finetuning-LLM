SELECT id, paid_at, order_id
FROM payments
WHERE order_id = 2 OR status IN ('REFUNDED', 'FAILED', 'SUCCESS')
ORDER BY paid_at;
