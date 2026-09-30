SELECT delivered_date
FROM shipments
WHERE YEAR(shipped_date) = 2023
UNION ALL
SELECT launch_date
FROM products
WHERE LENGTH(product_name) >= 7;
