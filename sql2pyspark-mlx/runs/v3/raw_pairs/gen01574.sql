SELECT store_id, SUM(quantity) AS total_quantity, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM inventory
GROUP BY store_id
