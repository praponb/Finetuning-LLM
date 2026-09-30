SELECT p.stock_qty, p.brand, p.category, s.supplier_name FROM products p LEFT JOIN suppliers s ON s.id = p.supplier_id ORDER BY brand ASC
