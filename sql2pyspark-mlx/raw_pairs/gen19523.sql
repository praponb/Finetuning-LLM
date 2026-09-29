SELECT status
FROM subscriptions
INTERSECT
SELECT status
FROM subscriptions
WHERE customer_id IS NULL;
