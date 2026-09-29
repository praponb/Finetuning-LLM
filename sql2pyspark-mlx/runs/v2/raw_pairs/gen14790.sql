SELECT id, status
FROM payments
WHERE amount BETWEEN 1220 AND 1520
UNION ALL
SELECT id, status
FROM payments
WHERE method IN ('card', 'bank_transfer')
