SELECT store_id, product_id, COUNT(*) AS cnt
FROM inventory
WHERE quantity > 95 AND store_id IN (1, 4, 5) AND quantity BETWEEN 182 AND 201
GROUP BY store_id, product_id
HAVING COUNT(quantity) >= 1
ORDER BY store_id DESC
