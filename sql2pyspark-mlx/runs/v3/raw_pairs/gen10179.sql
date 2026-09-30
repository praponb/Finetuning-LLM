SELECT p.cost, p.brand, s.supplier_name, s.city FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id;
