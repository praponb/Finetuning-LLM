SELECT status
FROM orders
WHERE status IS NOT NULL
UNION ALL
SELECT location status
FROM departments
WHERE location IS NOT NULL
ORDER BY status DESC;
