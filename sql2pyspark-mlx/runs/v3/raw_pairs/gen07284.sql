SELECT p.id, p.supplier_id, s.country, s.city, s.supplier_name FROM products p LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id WHERE p.price BETWEEN 360 AND 1040 ORDER BY s.city, id LIMIT 10;
