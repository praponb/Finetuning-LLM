SELECT id, method, COUNT(*) OVER (PARTITION BY method) AS method_count
FROM payments
WHERE HOUR(paid_at) BETWEEN 7 AND 15
