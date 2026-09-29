SELECT launch_date
FROM products
EXCEPT
SELECT shipped_date launch_date
FROM shipments
