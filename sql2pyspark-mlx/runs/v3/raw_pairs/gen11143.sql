SELECT store_id, MIN(sale_date) AS first_sale_date FROM store_sales GROUP BY store_id ORDER BY 2
