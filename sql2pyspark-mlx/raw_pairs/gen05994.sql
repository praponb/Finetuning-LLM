SELECT supplier_id, category
FROM products
INTERSECT ALL
SELECT supplier_id, category
FROM products
WHERE SIZE(tags) > 1
