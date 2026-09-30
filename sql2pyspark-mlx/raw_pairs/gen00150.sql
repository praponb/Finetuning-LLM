SELECT product_id, AVG(quantity) avg_quantity, MAX(quantity) - MIN(quantity) quantity_range
FROM inventory
WHERE store_id IN (1, 5) AND product_id = 2 AND quantity IS NOT NULL
GROUP BY 1;
