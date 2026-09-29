SELECT p.id, p.product_name, s.country, s.city, s.supplier_name FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id WHERE p.stock_qty BETWEEN 143 AND 392;
