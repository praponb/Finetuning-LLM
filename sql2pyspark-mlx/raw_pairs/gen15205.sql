SELECT MAX(quantity) AS max_quantity, ROUND(STDDEV(quantity), 2) AS stddev_quantity FROM order_items ORDER BY stddev_quantity, max_quantity;
