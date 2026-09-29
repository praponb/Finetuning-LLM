SELECT p.cost, s.rating FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id;
