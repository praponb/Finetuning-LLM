SELECT status
FROM accounts
INTERSECT
SELECT status
FROM subscriptions
ORDER BY status DESC;
