SELECT store_id, product_id, MIN(sale_date) AS first_sale_date FROM store_sales WHERE sale_date > '2024-06-01' GROUP BY 1, 2 ORDER BY first_sale_date DESC;
