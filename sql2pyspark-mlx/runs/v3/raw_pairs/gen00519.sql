SELECT p.id, p.supplier_id, p.price, s.city, s.supplier_name FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id;
