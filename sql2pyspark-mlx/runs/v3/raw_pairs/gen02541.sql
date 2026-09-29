SELECT p.id, p.supplier_id, p.launch_date, p.cost, s.rating, s.supplier_name, s.city FROM products p JOIN suppliers s ON s.id = p.supplier_id WHERE s.supplier_name NOT LIKE 'Eur%'
