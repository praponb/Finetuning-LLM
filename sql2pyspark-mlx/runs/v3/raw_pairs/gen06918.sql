SELECT store_id, AVG(quantity) AS avg_quantity
FROM inventory
WHERE quantity IS NOT NULL AND quantity BETWEEN 63 AND 101
GROUP BY store_id
