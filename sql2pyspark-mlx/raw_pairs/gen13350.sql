SELECT id, store_id, ROUND(quantity, 2) AS quantity_rounded
FROM inventory
WHERE store_id = 5 AND last_restock < '2023-04-01';
