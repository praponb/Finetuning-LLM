SELECT status
FROM payments
WHERE paid_at >= '2023-09-01'
UNION ALL
SELECT status
FROM subscriptions
WHERE status <> 'EXPIRED'
ORDER BY status;
