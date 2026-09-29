SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity FROM inventory WHERE last_restock > '2024-04-01' AND quantity BETWEEN 61 AND 99 GROUP BY product_id ORDER BY 1 DESC;
