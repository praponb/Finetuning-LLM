SELECT id, product_name, launch_date
FROM products
WHERE stock_qty != 380
INTERSECT
SELECT id, product_name, launch_date
FROM products
WHERE brand IS NOT NULL
ORDER BY product_name DESC;
