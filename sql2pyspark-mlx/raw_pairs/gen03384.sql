SELECT product_id, SUM(quantity) total_quantity, ROUND(STDDEV(quantity), 2) stddev_quantity
FROM inventory
WHERE store_id IN (2, 4)
GROUP BY product_id
HAVING ROUND(STDDEV(quantity), 2) < 80
ORDER BY stddev_quantity DESC;
