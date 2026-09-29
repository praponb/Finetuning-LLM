SELECT store_id, MIN(quantity) AS min_quantity, ROUND(STDDEV(quantity), 2) AS stddev_quantity FROM inventory GROUP BY store_id HAVING ROUND(STDDEV(quantity), 2) > 37 ORDER BY stddev_quantity DESC;
