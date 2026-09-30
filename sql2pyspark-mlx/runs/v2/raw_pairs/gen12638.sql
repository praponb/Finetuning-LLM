SELECT id, status
FROM payments
WHERE paid_at < '2023-06-01'
UNION
SELECT id, status
FROM payments
WHERE amount <= 420;
