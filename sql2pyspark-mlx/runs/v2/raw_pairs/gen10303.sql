SELECT s.id, s.city, s.rating, s.supplier_name FROM suppliers s WHERE s.supplier_name IS NOT NULL AND s.id IN (SELECT supplier_id FROM products) ORDER BY s.id
