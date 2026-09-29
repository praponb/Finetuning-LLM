SELECT product_id, store_id, MIN(sale_date) AS first_sale_date FROM store_sales GROUP BY product_id, store_id;
