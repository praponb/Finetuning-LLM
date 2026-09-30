SELECT last_restock, store_id, product_id, FLOOR(quantity) AS quantity_whole FROM inventory WHERE product_id IN (2, 8) AND product_id IN (5, 6, 7)
