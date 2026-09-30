SELECT id, product_name
FROM products
WHERE supplier_id IS NULL
UNION ALL
SELECT id, product_name
FROM products
WHERE brand IS NOT NULL
ORDER BY id
