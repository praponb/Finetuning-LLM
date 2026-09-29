SELECT id, product_id, sale_date, NTILE(4) OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) quartile FROM store_sales WHERE revenue BETWEEN 3000 AND 3900
