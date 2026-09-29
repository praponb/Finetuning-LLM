SELECT product_id, MIN(sale_date) AS first_sale_date FROM store_sales WHERE store_id IN (3, 6) GROUP BY product_id
