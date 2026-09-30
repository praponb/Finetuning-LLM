SELECT product_id, MIN(sale_date) AS min_sale_date FROM store_sales GROUP BY product_id;
