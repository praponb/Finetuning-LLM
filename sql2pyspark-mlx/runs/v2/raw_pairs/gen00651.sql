SELECT status
FROM accounts
WHERE status IN ('OPEN', 'FROZEN', 'CLOSED')
EXCEPT
SELECT status
FROM subscriptions;
