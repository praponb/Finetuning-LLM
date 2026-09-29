SELECT 'employee' AS kind, level
FROM employees
WHERE level IS NOT NULL
UNION ALL
SELECT 'store' AS kind, store_name
FROM stores
WHERE store_name IS NOT NULL;
