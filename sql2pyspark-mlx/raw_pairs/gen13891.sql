SELECT ss.product_id, ss.store_id, MAX(i.last_restock) AS latest_last_restock
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id IN (1, 2, 5, 7)
GROUP BY ss.product_id, ss.store_id
