SELECT s.supplier_name, s.rating, s.country, p.category, p.product_name FROM suppliers s LEFT JOIN products p ON s.id = p.supplier_id WHERE s.rating > 2.4 ORDER BY s.rating DESC, p.product_name
