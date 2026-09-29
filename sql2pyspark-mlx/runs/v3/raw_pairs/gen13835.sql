SELECT shipped_date
FROM shipments
UNION ALL
SELECT launch_date
FROM products
ORDER BY shipped_date
