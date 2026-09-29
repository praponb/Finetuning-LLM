SELECT p.launch_date, s.supplier_name FROM products p LEFT JOIN suppliers s ON s.id = p.supplier_id;
