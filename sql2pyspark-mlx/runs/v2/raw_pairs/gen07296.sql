SELECT s.id, s.country, s.city, s.supplier_name FROM suppliers s WHERE s.id IN (SELECT supplier_id FROM products WHERE stock_qty BETWEEN 234 AND 266) ORDER BY s.country, id LIMIT 5
