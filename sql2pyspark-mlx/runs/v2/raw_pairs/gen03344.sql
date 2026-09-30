SELECT p.id, p.cost, p.supplier_id, s.rating, s.city FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id WHERE s.supplier_name LIKE 'Gl%' ORDER BY id
