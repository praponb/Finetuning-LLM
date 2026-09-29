SELECT id, product_id, quantity FROM inventory WHERE store_id = 2 UNION ALL SELECT id, product_id, quantity FROM inventory WHERE quantity >= 104
