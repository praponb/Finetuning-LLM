SELECT p.id, p.stock_qty, p.cost, p.brand, s.city FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id WHERE s.supplier_name NOT LIKE 'T%' OR p.price >= 1060
