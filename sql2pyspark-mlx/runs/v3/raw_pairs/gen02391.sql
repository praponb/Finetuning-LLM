SELECT ss.store_id, ss.product_id, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id IN (1, 6, 10)
GROUP BY ss.store_id, ss.product_id
HAVING ROUND(STDDEV(i.quantity), 2) >= 28
