SELECT launch_date, brand
FROM products
WHERE supplier_id IS NOT NULL
UNION
SELECT launch_date, brand
FROM products
WHERE cost < 210
ORDER BY launch_date;
