SELECT status
FROM accounts
WHERE balance < 57000
INTERSECT
SELECT status
FROM shipments
ORDER BY status
