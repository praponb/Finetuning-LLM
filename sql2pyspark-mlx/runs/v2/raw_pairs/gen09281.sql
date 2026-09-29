SELECT quantity, product_id, last_restock
FROM inventory
WHERE product_id IN (2, 4, 7, 9)
