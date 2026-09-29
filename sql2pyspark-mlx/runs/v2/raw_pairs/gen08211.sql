SELECT id, status, order_id
FROM payments
WHERE status = 'SUCCESS' AND status = 'REFUNDED' AND order_id IN (5, 9, 16, 30)
ORDER BY id DESC
