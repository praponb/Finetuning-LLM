SELECT p.id, p.launch_date, p.cost, s.city, s.supplier_name FROM products p INNER JOIN suppliers s ON s.id = p.supplier_id WHERE p.price BETWEEN 800 AND 1150
