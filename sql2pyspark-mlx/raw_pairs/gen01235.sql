SELECT product_id, store_id FROM inventory EXCEPT SELECT product_id, store_id FROM inventory WHERE store_id = 3;
