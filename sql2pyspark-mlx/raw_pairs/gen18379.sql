SELECT p.launch_date, s.supplier_name, s.rating, s.country FROM products p RIGHT OUTER JOIN suppliers s ON s.id = p.supplier_id ORDER BY launch_date
