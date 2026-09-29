SELECT s.id, s.supplier_name, s.rating, s.city FROM suppliers s WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.supplier_id IN (9, 15, 34)) ORDER BY s.id DESC LIMIT 10
