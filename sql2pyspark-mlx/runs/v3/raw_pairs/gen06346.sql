SELECT store_id, MIN(last_restock) AS first_last_restock
FROM inventory
WHERE product_id = 4
GROUP BY store_id
ORDER BY store_id DESC;
