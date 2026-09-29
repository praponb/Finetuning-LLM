SELECT shipped_date
FROM shipments
WHERE shipped_date < '2024-05-01'
INTERSECT
SELECT launch_date
FROM products;
