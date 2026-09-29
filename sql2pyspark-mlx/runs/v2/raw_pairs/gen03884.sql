SELECT ss.product_id, ROUND(STDDEV(i.quantity), 2) stddev_quantity FROM inventory i LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id GROUP BY ss.product_id
