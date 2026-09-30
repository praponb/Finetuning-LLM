SELECT status
FROM accounts
UNION ALL
SELECT status
FROM subscriptions
ORDER BY status DESC
