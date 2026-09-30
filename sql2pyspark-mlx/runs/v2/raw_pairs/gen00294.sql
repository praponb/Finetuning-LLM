SELECT product_name
FROM products
WHERE category != 'Grocery'
UNION
SELECT product_name
FROM products
WHERE cost >= 400
ORDER BY product_name DESC
