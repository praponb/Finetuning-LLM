SELECT ss.store_id, SUM(i.quantity) total_quantity, MAX(i.quantity) - MIN(i.quantity) quantity_range
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.store_id
