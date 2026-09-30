SELECT last_restock, product_id, quantity
FROM inventory
WHERE store_id = 5 OR quantity = 154
