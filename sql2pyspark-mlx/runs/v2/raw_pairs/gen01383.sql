SELECT s.id, s.city, s.country FROM suppliers s WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id) ORDER BY country DESC;
