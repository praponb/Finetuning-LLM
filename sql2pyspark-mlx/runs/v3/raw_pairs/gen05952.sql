SELECT ss.store_id, COUNT(i.quantity) AS quantity_count, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.store_id IN (1, 3, 5, 6)
GROUP BY ss.store_id
ORDER BY quantity_count DESC;
