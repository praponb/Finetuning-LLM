SELECT i.id, i.store_id, p.stock_qty, p.cost, s.supplier_name FROM inventory i RIGHT JOIN products p ON i.product_id = p.id INNER JOIN suppliers s ON p.supplier_id = s.id ORDER BY stock_qty
