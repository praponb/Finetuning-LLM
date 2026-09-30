SELECT s.id, s.supplier_name, s.city, s.country FROM suppliers s WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id);
