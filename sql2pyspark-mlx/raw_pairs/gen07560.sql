SELECT store_id, COUNT(DISTINCT quantity) AS unique_products, MAX(quantity) AS max_quantity
FROM inventory
WHERE last_restock < '2024-04-01'
GROUP BY 1;
