SELECT product_name
FROM products
WHERE product_name IS NOT NULL
UNION
SELECT level AS product_name
FROM employees
WHERE level IS NOT NULL
