SELECT p.supplier_id, p.category, p.brand, s.city, s.supplier_name FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id WHERE p.stock_qty BETWEEN 112 AND 364 OR s.rating BETWEEN 2.7 AND 3.9
