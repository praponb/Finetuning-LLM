SELECT status
FROM shipments
UNION
SELECT status
FROM subscriptions
ORDER BY status DESC;
