SELECT status
FROM payments
WHERE HOUR(paid_at) BETWEEN 7 AND 11
INTERSECT
SELECT status
FROM subscriptions
