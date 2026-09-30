SELECT p.id, p.category, p.cost, s.supplier_name, s.rating FROM products p LEFT JOIN suppliers s ON s.id = p.supplier_id;
