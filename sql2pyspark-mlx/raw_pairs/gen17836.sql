SELECT p.brand, p.cost, p.product_name, s.country, s.supplier_name FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id
