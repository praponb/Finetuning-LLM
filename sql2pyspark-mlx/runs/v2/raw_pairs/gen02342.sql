SELECT id, supplier_id, product_name
FROM products
WHERE price >= 970
UNION
SELECT id, supplier_id, product_name
FROM products
WHERE ARRAY_CONTAINS(tags, 'eco')
