SELECT s.id, s.country, s.city, s.supplier_name FROM suppliers s WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.supplier_id IN (7, 20, 33));
