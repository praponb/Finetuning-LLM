SELECT i.quantity, ss.product_id, ss.store_id
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.quantity BETWEEN 95 AND 136 AND ss.product_id IN (1, 4, 8);
