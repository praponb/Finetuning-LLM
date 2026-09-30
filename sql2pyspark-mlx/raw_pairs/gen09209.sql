SELECT MAX(last_restock) AS last_last_restock FROM inventory WHERE store_id IN (2, 3, 5) ORDER BY last_last_restock
