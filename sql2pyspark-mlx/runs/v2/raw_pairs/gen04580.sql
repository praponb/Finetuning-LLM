SELECT p.id, p.stock_qty, p.price, s.city FROM products p FULL OUTER JOIN suppliers s ON s.id = p.supplier_id WHERE p.price BETWEEN 630 AND 660
