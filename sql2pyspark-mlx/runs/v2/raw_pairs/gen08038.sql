SELECT id, product_id, sale_date FROM store_sales WHERE units > 26 UNION SELECT id, product_id, sale_date FROM store_sales WHERE sale_date BETWEEN '2024-10-01' AND '2025-03-01'
