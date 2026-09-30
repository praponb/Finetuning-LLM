SELECT quantity FROM inventory WHERE store_id = 4 UNION SELECT quantity FROM inventory WHERE product_id IN (5, 6, 7);
