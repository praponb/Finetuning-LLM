SELECT i.id, i.last_restock, i.quantity, ss.store_id FROM inventory i LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id;
