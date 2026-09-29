SELECT p.product_name, p.brand, p.supplier_id, s.supplier_name, s.country FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id WHERE p.price > 810;
