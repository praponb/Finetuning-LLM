SELECT status
FROM payments
WHERE order_id IN (9, 12, 14, 28)
UNION
SELECT status
FROM subscriptions
ORDER BY status DESC
