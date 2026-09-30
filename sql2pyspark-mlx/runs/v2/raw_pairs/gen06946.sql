SELECT COUNT(quantity) AS quantity_count
FROM inventory
WHERE product_id IN (6, 10) AND last_restock > '2023-07-01' AND store_id IN (1, 3, 5, 6);
