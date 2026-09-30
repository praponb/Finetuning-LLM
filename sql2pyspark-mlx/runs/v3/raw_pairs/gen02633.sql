SELECT product_id, sale_date FROM store_sales WHERE product_id IN (2, 5, 6) UNION SELECT product_id, sale_date FROM store_sales WHERE sale_date < '2024-09-16' ORDER BY product_id
