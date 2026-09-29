SELECT status
FROM accounts
WHERE status IS NOT NULL
INTERSECT
SELECT status
FROM shipments
ORDER BY status;
