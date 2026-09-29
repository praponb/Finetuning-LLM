SELECT p.product_name, p.stock_qty, p.price, i.store_id, i.quantity FROM products p LEFT OUTER JOIN inventory i ON i.product_id = p.id;
