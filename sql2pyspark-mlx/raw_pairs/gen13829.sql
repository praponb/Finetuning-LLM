SELECT i.last_restock, i.store_id, ss.product_id
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id IN (2, 3) OR i.store_id = 4;
