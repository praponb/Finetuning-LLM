SELECT id, last_restock, product_id
FROM inventory
WHERE MONTH(last_restock) IN (3, 5, 11);
