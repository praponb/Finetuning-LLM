SELECT product_id, store_id FROM inventory WHERE store_id IN (1, 3) INTERSECT SELECT product_id, store_id FROM inventory WHERE product_id IN (1, 3, 5, 7);
