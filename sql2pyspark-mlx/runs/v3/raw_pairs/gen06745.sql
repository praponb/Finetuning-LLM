SELECT store_id, COUNT(quantity) AS quantity_count, MAX(quantity) AS max_quantity
FROM inventory
GROUP BY store_id
HAVING COUNT(quantity) < 2
