SELECT product_id, ROUND(STDDEV(quantity), 2) stddev_quantity FROM inventory GROUP BY 1
