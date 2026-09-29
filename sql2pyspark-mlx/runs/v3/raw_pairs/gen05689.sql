SELECT s.id, s.rating, s.country, s.city FROM suppliers s WHERE LOWER(s.supplier_name) LIKE '%lob%' AND s.id IN (SELECT supplier_id FROM products WHERE supplier_id IS NOT NULL) ORDER BY city;
