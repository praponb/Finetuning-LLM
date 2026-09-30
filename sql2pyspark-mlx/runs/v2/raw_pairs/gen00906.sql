SELECT product_id, COUNT(*) AS total_count, MIN(last_restock) AS min_last_restock
FROM inventory
WHERE last_restock >= '2024-09-01'
GROUP BY product_id;
