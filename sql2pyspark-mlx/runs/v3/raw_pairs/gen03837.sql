SELECT p.product_name, p.brand, p.price, s.rating, s.supplier_name, s.country FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id ORDER BY s.country
