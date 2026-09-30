SELECT method
FROM payments
WHERE paid_at >= '2023-10-01'
UNION ALL
SELECT method
FROM payments
WHERE paid_at < '2023-03-22'
ORDER BY method DESC;
