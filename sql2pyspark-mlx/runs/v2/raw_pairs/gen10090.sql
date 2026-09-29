SELECT status
FROM accounts
EXCEPT
SELECT status
FROM subscriptions
WHERE status = 'ACTIVE'
ORDER BY status DESC;
