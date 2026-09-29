SELECT p.id, p.brand, p.stock_qty, s.city, s.country, LOWER(s.supplier_name) supplier_name_lower FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id WHERE p.price >= 940 ORDER BY id DESC
