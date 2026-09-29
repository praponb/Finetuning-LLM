SELECT id, last_restock, store_id
FROM inventory
WHERE MONTH(last_restock) IN (6, 9, 10) OR last_restock >= '2025-01-01'
ORDER BY last_restock DESC;
