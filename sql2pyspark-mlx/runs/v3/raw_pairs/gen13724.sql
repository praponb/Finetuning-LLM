SELECT status
FROM payments
WHERE status IN ('SUCCESS', 'REFUNDED', 'FAILED')
UNION
SELECT status
FROM subscriptions;
