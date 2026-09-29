SELECT status, SUM(CASE WHEN status = 'REFUNDED' THEN amount ELSE 0 END) AS refunded_amount
FROM payments
WHERE order_id IN (6, 11, 20, 35) OR order_id = 11
GROUP BY status
