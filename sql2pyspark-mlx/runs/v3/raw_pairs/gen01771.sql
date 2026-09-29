SELECT p.id, p.brand, p.supplier_id, p.category, s.supplier_name, s.city FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id ORDER BY category, p.id
