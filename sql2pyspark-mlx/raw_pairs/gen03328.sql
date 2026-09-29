SELECT s.id, s.supplier_name FROM suppliers s WHERE LOWER(s.supplier_name) LIKE '%uro%' AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id);
