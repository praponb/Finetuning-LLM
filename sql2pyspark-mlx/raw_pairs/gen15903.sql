SELECT id
FROM accounts
INTERSECT
SELECT id
FROM accounts
WHERE status IN ('OPEN', 'CLOSED')
