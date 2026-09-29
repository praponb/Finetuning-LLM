SELECT id, method
FROM payments
WHERE status IN ('FAILED', 'SUCCESS')
UNION ALL
SELECT id, method
FROM payments
WHERE TO_DATE(paid_at) < '2024-01-23'
ORDER BY method DESC;
