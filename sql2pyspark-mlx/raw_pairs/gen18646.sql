SELECT store_id, product_id FROM inventory WHERE quantity <= 141 INTERSECT SELECT store_id, product_id FROM inventory WHERE store_id IN (1, 2, 5);
