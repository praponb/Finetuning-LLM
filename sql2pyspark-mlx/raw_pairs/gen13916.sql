SELECT i.id, i.last_restock, i.quantity, p.stock_qty, p.cost FROM inventory i INNER JOIN products p ON i.product_id = p.id ORDER BY id DESC;
