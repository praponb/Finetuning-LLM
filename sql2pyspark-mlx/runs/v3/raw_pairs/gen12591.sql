SELECT status, 'payment' AS source
FROM payments
WHERE order_id IN (4, 22, 35)
UNION ALL
SELECT status, 'subscription' AS source
FROM subscriptions
ORDER BY source
