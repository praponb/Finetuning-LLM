SELECT status, 'account' AS source
FROM accounts
WHERE balance <= 63000
UNION
SELECT status, 'subscription' AS source
FROM subscriptions
ORDER BY status DESC;
