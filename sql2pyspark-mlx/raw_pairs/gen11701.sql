SELECT status
FROM accounts
WHERE status != 'FROZEN'
INTERSECT
SELECT status
FROM payments
ORDER BY status;
