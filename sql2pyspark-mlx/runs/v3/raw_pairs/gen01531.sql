SELECT id, status, method
FROM payments
WHERE method = 'card'
UNION ALL
SELECT id, status, method
FROM payments
WHERE amount >= 620
ORDER BY status DESC
