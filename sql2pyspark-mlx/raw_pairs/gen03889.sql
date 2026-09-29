SELECT product_id, MIN(quantity) AS min_quantity, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM inventory
WHERE product_id IN (2, 3, 4, 5)
GROUP BY 1;
