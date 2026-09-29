SELECT account_type
FROM accounts
WHERE status NOT IN ('FROZEN', 'CLOSED', 'OPEN')
ORDER BY account_type
