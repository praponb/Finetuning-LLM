SELECT id, method, COUNT(*) OVER (PARTITION BY method) AS method_count
FROM payments
