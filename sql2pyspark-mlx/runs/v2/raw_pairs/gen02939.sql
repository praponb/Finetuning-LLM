SELECT ss.store_id, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity, MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.store_id
ORDER BY 2, store_id
LIMIT 3
