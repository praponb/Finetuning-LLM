SELECT MIN(sale_date) AS min_sale_date FROM store_sales WHERE store_id = 4 AND units >= 24 ORDER BY min_sale_date
