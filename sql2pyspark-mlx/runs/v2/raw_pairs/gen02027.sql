SELECT ss.store_id, MIN(i.quantity) AS min_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.store_id IN (1, 2, 3, 5)
GROUP BY ss.store_id;
