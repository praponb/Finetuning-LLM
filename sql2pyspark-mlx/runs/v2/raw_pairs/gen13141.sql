SELECT product_id, MONTH(sale_date) AS sale_month, MAX(sale_date) AS last_sale_date FROM store_sales WHERE store_id IN (1, 3, 4, 6) GROUP BY product_id, sale_month
