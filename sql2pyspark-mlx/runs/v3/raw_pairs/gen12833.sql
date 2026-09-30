SELECT s.rating, s.supplier_name, s.country, p.cost FROM suppliers s LEFT JOIN products p ON s.id = p.supplier_id ORDER BY s.supplier_name, rating DESC
