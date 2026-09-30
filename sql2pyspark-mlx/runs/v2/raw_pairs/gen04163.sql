SELECT id, status
FROM accounts
WHERE status = 'OPEN'
UNION ALL
SELECT id, status
FROM accounts
WHERE customer_id IS NULL
ORDER BY status DESC;
