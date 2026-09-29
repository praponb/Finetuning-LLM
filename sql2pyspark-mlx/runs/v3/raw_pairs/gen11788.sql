SELECT store_id, MAX(last_restock) AS last_last_restock, ROUND(STDDEV(quantity), 2) AS stddev_quantity FROM inventory WHERE quantity >= 216 GROUP BY store_id;
