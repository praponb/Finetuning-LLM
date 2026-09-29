SELECT p.id, p.launch_date, s.city, s.supplier_name, s.country FROM products p RIGHT JOIN suppliers s ON p.supplier_id = s.id ORDER BY country;
