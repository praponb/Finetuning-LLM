SELECT p.id, p.product_name, p.category, s.supplier_name, s.rating FROM products p LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id ORDER BY s.rating DESC
