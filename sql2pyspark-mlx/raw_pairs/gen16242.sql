SELECT p.id, p.stock_qty, p.product_name, s.city, s.country, s.supplier_name FROM products p JOIN suppliers s ON p.supplier_id = s.id WHERE p.cost >= 590
