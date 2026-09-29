SELECT launch_date
FROM products
WHERE ARRAY_CONTAINS(tags, 'new')
UNION ALL
SELECT launch_date
FROM products
WHERE MONTH(launch_date) IN (1, 2, 8)
ORDER BY launch_date
