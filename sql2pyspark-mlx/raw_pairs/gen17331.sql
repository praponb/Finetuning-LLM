SELECT COUNT(quantity) AS quantity_count
FROM inventory
WHERE store_id IN (2, 3, 4, 5);
