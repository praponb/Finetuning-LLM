SELECT id, last_restock, RANK() OVER (ORDER BY last_restock) AS rnk
FROM inventory
WHERE store_id IN (1, 5, 6);
