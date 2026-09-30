SELECT ss.store_id, ROUND(AVG(i.quantity), 2) AS avg_quantity
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id IN (2, 5, 9)
GROUP BY ss.store_id
ORDER BY ss.store_id DESC, avg_quantity;
