SELECT *
FROM inventory
WHERE last_restock < '2025-01-01' AND product_id = 5
