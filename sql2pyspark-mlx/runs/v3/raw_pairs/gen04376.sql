SELECT i.id, i.quantity, ss.product_id FROM inventory i LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE ss.product_id IN (4, 10) ORDER BY id;
