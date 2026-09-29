SELECT product_id, MIN(last_restock) AS first_last_restock
FROM inventory
WHERE last_restock <= '2023-10-23'
GROUP BY product_id
ORDER BY product_id;
