SELECT i.id, i.product_id, p.stock_qty, s.country FROM inventory i FULL OUTER JOIN products p ON p.id = i.product_id JOIN suppliers s ON s.id = p.supplier_id ORDER BY p.stock_qty DESC
