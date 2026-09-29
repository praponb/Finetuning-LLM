SELECT ss.product_id, ss.store_id, AVG(i.quantity) AS avg_quantity
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.product_id, ss.store_id
ORDER BY avg_quantity DESC
