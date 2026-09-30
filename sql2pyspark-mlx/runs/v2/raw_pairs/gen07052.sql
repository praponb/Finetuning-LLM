SELECT status
FROM payments
INTERSECT
SELECT status
FROM subscriptions
ORDER BY status;
