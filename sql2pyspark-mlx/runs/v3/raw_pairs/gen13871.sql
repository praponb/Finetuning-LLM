SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, MAX(quantity) AS max_quantity
FROM inventory
GROUP BY product_id
HAVING COUNT(quantity) >= 4
ORDER BY max_quantity DESC;
