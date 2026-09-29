SELECT ss.product_id, COUNT(i.quantity) quantity_count, MAX(i.last_restock) last_last_restock
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id IN (5, 6, 8)
GROUP BY ss.product_id;
