SELECT level
FROM employees
WHERE NOT is_remote
UNION
SELECT channel level
FROM orders
WHERE discount > 0.21
ORDER BY level;
