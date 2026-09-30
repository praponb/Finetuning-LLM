SELECT p.stock_qty, p.category, s.supplier_name, s.city FROM products p LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id ORDER BY category ASC
