SELECT store_id, quantity, product_id, ROUND(quantity * 100, 2) adjusted_quantity FROM inventory WHERE product_id IN (4, 5, 10);
