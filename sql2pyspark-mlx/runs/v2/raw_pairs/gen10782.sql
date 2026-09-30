SELECT ss.store_id, MAX(i.quantity) AS max_quantity, MIN(i.last_restock) AS min_last_restock
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.store_id
ORDER BY ss.store_id
