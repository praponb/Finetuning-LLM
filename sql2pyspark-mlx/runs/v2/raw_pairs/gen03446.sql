SELECT p.id, p.brand, p.supplier_id, s.city, s.rating, s.supplier_name FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id WHERE s.rating BETWEEN 2.2 AND 3.6
