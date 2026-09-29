SELECT quantity FROM inventory WHERE product_id IN (1, 2) UNION SELECT quantity FROM inventory WHERE store_id IN (1, 3, 6) ORDER BY quantity
