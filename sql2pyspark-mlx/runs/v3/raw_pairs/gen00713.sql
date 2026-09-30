SELECT shipped_date
FROM shipments
UNION
SELECT end_date
FROM subscriptions
WHERE status = 'CANCELLED'
