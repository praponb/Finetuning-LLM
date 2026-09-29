SELECT status
FROM shipments
UNION ALL
SELECT status
FROM subscriptions
WHERE end_date >= '2025-07-01'
ORDER BY status
