SELECT s.id, s.city, s.supplier_name
FROM suppliers s
WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.category IN ('Grocery', 'Sports', 'Electronics'))
ORDER BY city, id
LIMIT 10;
