SELECT id, status
FROM payments
WHERE amount < 990
UNION
SELECT id, status
FROM payments
WHERE status NOT IN ('SUCCESS', 'REFUNDED')
ORDER BY status;
