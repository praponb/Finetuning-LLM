SELECT store_id, product_id, MAX(last_restock) AS last_last_restock
FROM inventory
WHERE product_id IN (8, 9)
GROUP BY 1, 2
