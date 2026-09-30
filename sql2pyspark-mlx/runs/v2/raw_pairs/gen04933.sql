SELECT s.id, s.country, s.city FROM suppliers s WHERE s.id IN (SELECT supplier_id FROM products);
