SELECT store_id, MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
GROUP BY store_id
HAVING MAX(quantity) < 190;
