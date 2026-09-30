SELECT id, method
FROM payments
WHERE amount BETWEEN 1060 AND 1390
UNION ALL
SELECT id, method
FROM payments
WHERE paid_at < '2024-02-25';
