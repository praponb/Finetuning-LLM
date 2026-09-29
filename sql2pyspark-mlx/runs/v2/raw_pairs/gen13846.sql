SELECT status, method
FROM payments
WHERE paid_at < '2025-01-16'
UNION
SELECT status, method
FROM payments
WHERE HOUR(paid_at) BETWEEN 11 AND 19;
