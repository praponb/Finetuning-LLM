SELECT store_id, COUNT(*) total_count, MIN(sale_date) first_sale_date FROM store_sales GROUP BY store_id HAVING total_count >= 3;
