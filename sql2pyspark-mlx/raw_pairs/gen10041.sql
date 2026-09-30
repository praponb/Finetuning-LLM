SELECT p.cost, p.brand, s.city FROM products p LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id WHERE p.stock_qty = 271 OR s.city = 'London';
