SELECT p.stock_qty, p.category, s.supplier_name
FROM products p
JOIN suppliers s ON p.supplier_id = s.id;
