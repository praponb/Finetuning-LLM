SELECT category
FROM products
WHERE LENGTH(product_name) > 7
INTERSECT
SELECT category
FROM products
WHERE brand = 'Initech'
