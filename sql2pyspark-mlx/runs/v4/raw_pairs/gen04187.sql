SELECT id, method, COUNT(*) OVER (PARTITION BY method) method_count
FROM payments;
