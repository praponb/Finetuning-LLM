SELECT p.id, p.supplier_id, p.stock_qty, s.supplier_name FROM products p INNER JOIN suppliers s ON s.id = p.supplier_id WHERE p.supplier_id = 31
