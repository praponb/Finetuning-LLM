SELECT id, status
FROM accounts
WHERE status NOT IN ('CLOSED', 'OPEN')
ORDER BY id DESC;
