SELECT p.stock_qty, s.rating, s.city, s.country FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id WHERE s.city != 'Tokyo'
