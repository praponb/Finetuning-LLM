SELECT s.id, s.city
FROM suppliers s
WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.supplier_id IN (12, 15, 23, 38))
ORDER BY id;
