SELECT last_restock, product_id, quantity
FROM inventory
WHERE last_restock < '2025-01-27'
ORDER BY last_restock;
