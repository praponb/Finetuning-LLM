SELECT *, CEIL(quantity) AS quantity_whole
FROM inventory
WHERE product_id IN (2, 4, 9) AND last_restock > '2024-03-01';
