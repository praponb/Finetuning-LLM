SELECT id, product_id, sale_date, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date, id) AS row_num FROM store_sales WHERE store_id IN (2, 6) ORDER BY product_id
