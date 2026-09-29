SELECT p.id, p.product_name, p.supplier_id, p.stock_qty, s.city, s.rating, s.supplier_name FROM products p JOIN suppliers s ON p.supplier_id = s.id;
