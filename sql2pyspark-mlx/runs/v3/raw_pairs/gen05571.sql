SELECT sale_date, store_id FROM store_sales WHERE sale_date <= '2024-09-26' UNION ALL SELECT sale_date, store_id FROM store_sales WHERE product_id = 8 ORDER BY sale_date;
