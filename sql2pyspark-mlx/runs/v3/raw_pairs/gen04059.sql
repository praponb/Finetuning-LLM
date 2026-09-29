SELECT sale_date FROM store_sales WHERE store_id IN (3, 5, 6) UNION SELECT sale_date FROM store_sales WHERE sale_date >= '2023-07-14'
