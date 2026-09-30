SELECT id, method
FROM payments
WHERE paid_at < '2024-06-01'
UNION ALL
SELECT id, method
FROM payments
WHERE amount IS NOT NULL
