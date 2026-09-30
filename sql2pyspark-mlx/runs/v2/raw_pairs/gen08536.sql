SELECT s.id, s.city FROM suppliers s WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id) ORDER BY city ASC, id ASC LIMIT 10;
