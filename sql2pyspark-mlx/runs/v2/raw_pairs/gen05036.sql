SELECT * FROM store_sales WHERE (units IS NOT NULL AND product_id = 1) OR store_id IN (1, 4, 5, 6);
