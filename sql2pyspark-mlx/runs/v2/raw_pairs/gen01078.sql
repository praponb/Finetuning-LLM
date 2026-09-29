SELECT store_id, CAST(units AS STRING) units_num, COALESCE(revenue, 0) revenue_filled FROM store_sales WHERE sale_date < '2023-04-05' AND product_id IN (3, 5, 8, 9)
