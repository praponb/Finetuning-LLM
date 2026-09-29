SELECT s.id, s.city FROM suppliers s WHERE s.city IN ('Berlin', 'London', 'Singapore') AND s.id IN (SELECT supplier_id FROM products WHERE supplier_id IN (11, 27, 37));
