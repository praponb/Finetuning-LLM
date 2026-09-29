SELECT store_id, product_id FROM store_sales INTERSECT SELECT store_id, product_id FROM store_sales WHERE store_id IN (1, 2, 3, 6);
