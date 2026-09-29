SELECT launch_date, supplier_id
FROM products
WHERE product_name LIKE '%Chai%'
INTERSECT
SELECT launch_date, supplier_id
FROM products
WHERE price <= 780
ORDER BY launch_date;
