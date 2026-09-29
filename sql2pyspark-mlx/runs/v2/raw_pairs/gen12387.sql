SELECT p.id, p.product_name, s.country, s.supplier_name, s.city FROM products p LEFT JOIN suppliers s ON s.id = p.supplier_id
