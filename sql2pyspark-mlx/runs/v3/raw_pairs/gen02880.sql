SELECT id, product_id, sale_date, NTILE(2) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS quartile FROM store_sales
