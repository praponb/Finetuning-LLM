SELECT store_id, ROUND(STDDEV(units), 2) AS stddev_units FROM store_sales WHERE store_id IN (1, 2, 3, 4) GROUP BY 1
