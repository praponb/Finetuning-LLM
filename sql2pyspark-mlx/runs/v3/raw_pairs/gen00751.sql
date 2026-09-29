SELECT id, product_id FROM store_sales WHERE product_id = 3 INTERSECT SELECT id, product_id FROM store_sales WHERE units > 31;
