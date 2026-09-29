SELECT store_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, COUNT(DISTINCT quantity) AS unique_products FROM inventory WHERE last_restock >= '2025-03-01' GROUP BY store_id ORDER BY 1;
