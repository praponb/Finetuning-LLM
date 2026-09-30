SELECT store_id, MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
WHERE store_id IN (5, 6) AND quantity = 85
GROUP BY store_id;
