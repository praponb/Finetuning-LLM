SELECT id, product_id FROM inventory WHERE store_id IN (1, 2, 3, 6) UNION ALL SELECT id, product_id FROM inventory WHERE quantity < 128
