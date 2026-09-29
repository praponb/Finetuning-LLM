SELECT status, MIN(paid_at) AS min_paid_at
FROM payments
WHERE order_id IN (20, 32) AND order_id = 36
GROUP BY status;
