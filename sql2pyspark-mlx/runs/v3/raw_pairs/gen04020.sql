SELECT ss.product_id, ROUND(STDDEV(i.quantity), 2) stddev_quantity, MAX(i.quantity) max_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.product_id
