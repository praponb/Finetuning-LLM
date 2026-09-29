SELECT id, last_restock, ROUND(quantity, 1) quantity_rounded
FROM inventory
WHERE quantity = 162 AND quantity <= 84 AND product_id IN (1, 3, 7, 8)
