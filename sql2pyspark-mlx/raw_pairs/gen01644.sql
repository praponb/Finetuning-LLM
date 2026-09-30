SELECT ss.product_id, MAX(i.last_restock) AS latest_last_restock, SUM(i.quantity) AS total_quantity
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.product_id;
