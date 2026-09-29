SELECT store_id, SUM(quantity) AS total_quantity
FROM inventory
WHERE quantity <= 122 AND store_id IN (3, 6) AND product_id IN (6, 7, 8)
GROUP BY store_id
HAVING MAX(quantity) < 170
ORDER BY total_quantity, store_id;
