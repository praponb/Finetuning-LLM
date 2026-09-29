SELECT method
FROM payments
WHERE TO_DATE(paid_at) >= '2025-05-30'
EXCEPT
SELECT method
FROM payments
WHERE paid_at < '2023-10-13'
ORDER BY method
