SELECT units FROM store_sales WHERE product_id = 1 UNION ALL SELECT units FROM store_sales WHERE MONTH(sale_date) IN (4, 6, 11)
