SELECT id, sale_date, product_id, revenue FROM store_sales WHERE units >= 12 AND product_id IN (6, 8, 9, 10) ORDER BY id DESC
