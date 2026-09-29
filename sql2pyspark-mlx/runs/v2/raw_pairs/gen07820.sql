SELECT ss.product_id, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity, SUM(i.quantity) AS total_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.product_id
ORDER BY stddev_quantity DESC, product_id ASC
LIMIT 5
