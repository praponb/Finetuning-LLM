SELECT status, 'payment' AS source
FROM payments
WHERE status IN ('FAILED', 'REFUNDED')
UNION
SELECT status, 'subscription' AS source
FROM subscriptions;
