SELECT s.id, s.supplier_name FROM suppliers s WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id) ORDER BY supplier_name ASC, id ASC LIMIT 50;
