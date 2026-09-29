SELECT product_id, MIN(last_restock) AS min_last_restock
FROM inventory
WHERE last_restock > '2025-03-01'
GROUP BY product_id;
