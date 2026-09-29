SELECT sale_date, store_id FROM store_sales WHERE revenue < 3700 UNION SELECT sale_date, store_id FROM store_sales WHERE sale_date >= '2024-06-14'
