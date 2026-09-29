SELECT product_id, quantity FROM inventory WHERE store_id IN (2, 3, 6) UNION SELECT product_id, quantity FROM inventory WHERE quantity <= 77
