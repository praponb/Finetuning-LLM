SELECT s.id, s.city, s.country FROM suppliers s WHERE s.id IN (SELECT supplier_id FROM products WHERE supplier_id IN (3, 4, 27, 39)) ORDER BY s.city
