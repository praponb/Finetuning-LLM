SELECT store_id, MIN(last_restock) AS min_last_restock
FROM inventory
WHERE product_id IN (4, 10)
GROUP BY store_id
ORDER BY store_id DESC;
