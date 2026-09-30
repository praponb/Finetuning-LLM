SELECT p.id, p.launch_date, s.country, s.rating, s.city FROM products p LEFT JOIN suppliers s ON s.id = p.supplier_id WHERE p.cost > 240 ORDER BY p.id;
