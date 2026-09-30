SELECT status
FROM subscriptions
INTERSECT
SELECT status
FROM subscriptions
WHERE status = 'CANCELLED';
