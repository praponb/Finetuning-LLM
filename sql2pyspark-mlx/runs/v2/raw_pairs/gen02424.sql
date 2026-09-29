SELECT s.id, s.supplier_name, s.city FROM suppliers s WHERE s.id IN (SELECT supplier_id FROM products WHERE category != 'Home') ORDER BY supplier_name, id LIMIT 5;
