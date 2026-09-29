SELECT id, product_name, category
FROM products
WHERE ARRAY_CONTAINS(tags, 'sale')
UNION
SELECT id, product_name, category
FROM products
WHERE cost IS NULL
ORDER BY id
