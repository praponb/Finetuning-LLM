SELECT p.id, p.stock_qty, p.price, s.country, s.supplier_name, s.rating FROM products p FULL OUTER JOIN suppliers s ON s.id = p.supplier_id
