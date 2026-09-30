SELECT store_id, AVG(quantity) avg_quantity, ROUND(STDDEV(quantity), 2) stddev_quantity FROM inventory GROUP BY store_id ORDER BY stddev_quantity DESC, avg_quantity DESC;
