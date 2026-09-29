SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM inventory
WHERE product_id IN (3, 6, 7)
GROUP BY product_id
HAVING COUNT(DISTINCT quantity) > 4
ORDER BY stddev_quantity DESC;
