SELECT ss.store_id, MAX(i.quantity) AS max_quantity, MIN(i.quantity) AS min_quantity
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.quantity > 118
GROUP BY ss.store_id
ORDER BY min_quantity DESC;
