SELECT name
FROM customers
UNION ALL
SELECT product_name AS name
FROM products
WHERE product_name NOT LIKE '%Mak%'
