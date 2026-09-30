SELECT store_id, DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month, ROUND(STDDEV(quantity), 2) AS stddev_quantity FROM inventory WHERE product_id = 4 GROUP BY store_id, last_restock_month;
