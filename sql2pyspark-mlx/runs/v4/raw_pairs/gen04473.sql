SELECT customer_id, status
FROM subscriptions
INTERSECT
SELECT customer_id, status
FROM subscriptions
WHERE status = 'ACTIVE';
