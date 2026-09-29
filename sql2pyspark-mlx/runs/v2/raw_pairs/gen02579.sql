SELECT store_id, COUNT(DISTINCT quantity) unique_products
FROM inventory
WHERE NOT (store_id IN (1, 3, 4, 6) AND (product_id IN (1, 5, 8, 9) OR last_restock > '2023-09-01'))
GROUP BY store_id;
