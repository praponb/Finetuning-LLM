SELECT store_id, COUNT(quantity) quantity_count
FROM inventory
WHERE MONTH(last_restock) IN (4, 5, 7)
GROUP BY store_id
HAVING quantity_count >= 4
