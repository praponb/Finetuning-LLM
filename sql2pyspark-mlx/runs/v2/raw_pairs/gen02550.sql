SELECT SUM(quantity) AS total_quantity, COUNT(*) AS inventory_count
FROM inventory
WHERE last_restock >= '2023-08-26'
ORDER BY inventory_count DESC
