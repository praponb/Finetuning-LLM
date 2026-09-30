SELECT product_id, store_id, SUM(revenue) AS total_revenue, MIN(sale_date) AS min_sale_date FROM store_sales GROUP BY product_id, store_id;
