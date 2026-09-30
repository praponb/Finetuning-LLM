SELECT store_id, product_id FROM inventory WHERE quantity < 70 UNION SELECT store_id, product_id FROM inventory WHERE quantity >= 62
