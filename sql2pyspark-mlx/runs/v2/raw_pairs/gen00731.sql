SELECT status, 'account' AS source
FROM accounts
UNION
SELECT status, 'subscription' AS source
FROM subscriptions
ORDER BY status DESC
