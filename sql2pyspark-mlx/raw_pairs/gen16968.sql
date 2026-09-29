SELECT id, supplier_id
FROM products
WHERE supplier_id = 14
UNION ALL
SELECT id, supplier_id
FROM products
WHERE category = 'Electronics';
