SELECT p.id, p.product_name, s.supplier_name, s.country, s.rating FROM products p JOIN suppliers s ON p.supplier_id = s.id;
