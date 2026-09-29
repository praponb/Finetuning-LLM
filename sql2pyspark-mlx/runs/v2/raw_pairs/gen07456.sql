SELECT status
FROM accounts
UNION ALL
SELECT status
FROM shipments
WHERE status IN ('RETURNED', 'LOST', 'IN_TRANSIT')
