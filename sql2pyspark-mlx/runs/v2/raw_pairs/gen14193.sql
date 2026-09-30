SELECT status, order_id
FROM payments
WHERE status IN ('REFUNDED', 'SUCCESS')
ORDER BY status;
