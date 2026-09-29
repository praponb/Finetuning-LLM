SELECT store_id, product_id, MIN(sale_date) AS first_sale_date FROM store_sales GROUP BY store_id, product_id ORDER BY first_sale_date;
