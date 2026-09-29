SELECT status
FROM shipments
UNION
SELECT status
FROM subscriptions
WHERE customer_id IS NULL;
