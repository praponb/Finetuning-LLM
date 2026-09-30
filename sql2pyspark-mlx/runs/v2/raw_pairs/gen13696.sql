SELECT store_id, product_id FROM store_sales WHERE units <> 32 INTERSECT SELECT store_id, product_id FROM store_sales WHERE product_id IN (1, 2) ORDER BY product_id DESC;
