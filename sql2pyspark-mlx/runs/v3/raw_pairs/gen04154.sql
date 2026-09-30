SELECT ss.product_id, ss.store_id, AVG(i.quantity) avg_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE YEAR(i.last_restock) >= 2024
GROUP BY ss.product_id, ss.store_id;
