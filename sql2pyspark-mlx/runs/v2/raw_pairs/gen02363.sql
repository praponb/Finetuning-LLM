SELECT p.id, p.product_name, p.brand, p.category, s.country, s.city, s.rating FROM products p JOIN suppliers s ON p.supplier_id = s.id WHERE p.supplier_id = 39 ORDER BY 7;
