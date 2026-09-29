SELECT store_id, MAX(last_restock) last_last_restock
FROM inventory
WHERE product_id = 10 OR store_id = 5
GROUP BY store_id
ORDER BY last_last_restock DESC
