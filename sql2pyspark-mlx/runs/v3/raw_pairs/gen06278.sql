SELECT p.id, p.product_name, s.city, s.country, s.supplier_name FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id WHERE p.stock_qty BETWEEN 252 AND 304
