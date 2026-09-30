SELECT ROUND(AVG(quantity), 2) AS avg_quantity
FROM inventory
WHERE store_id = 3 AND quantity <= 135;
