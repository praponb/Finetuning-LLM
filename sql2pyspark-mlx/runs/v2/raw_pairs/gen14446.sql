SELECT p.cost, p.stock_qty, i.quantity, i.store_id, i.product_id FROM products p LEFT JOIN inventory i ON i.product_id = p.id WHERE i.product_id IN (2, 3, 4, 6) OR p.cost > 550;
