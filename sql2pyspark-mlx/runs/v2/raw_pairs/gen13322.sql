SELECT product_id, YEAR(last_restock) AS last_restock_year, ROUND(STDDEV(quantity), 2) AS stddev_quantity FROM inventory GROUP BY 1, 2
