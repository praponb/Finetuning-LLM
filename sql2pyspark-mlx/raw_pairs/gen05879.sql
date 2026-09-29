SELECT AVG(quantity) AS avg_quantity, MAX(last_restock) AS latest_last_restock
FROM inventory
WHERE product_id IN (1, 5, 6, 9)
ORDER BY avg_quantity DESC;
