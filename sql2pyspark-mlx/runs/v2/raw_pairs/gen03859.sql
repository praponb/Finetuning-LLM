SELECT i.last_restock, s.sqft, p.price, p.stock_qty FROM inventory i INNER JOIN stores s ON s.id = i.store_id INNER JOIN products p ON i.product_id = p.id;
