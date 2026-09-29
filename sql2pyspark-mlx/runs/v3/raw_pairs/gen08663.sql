SELECT p.id, p.product_name, p.brand, s.country, s.rating FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id WHERE p.category = 'Grocery' OR s.country = 'JP';
