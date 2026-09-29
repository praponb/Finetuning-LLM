SELECT p.id, p.category, p.stock_qty, s.supplier_name, s.rating, s.country FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id WHERE s.country != 'DE'
