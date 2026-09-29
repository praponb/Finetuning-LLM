SELECT id, sale_date FROM store_sales WHERE product_id IN (1, 8) AND product_id IN (1, 2, 5, 9) ORDER BY id LIMIT 1
