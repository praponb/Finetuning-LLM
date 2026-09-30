SELECT product_id, MAX(sale_date) AS last_sale_date, MIN(sale_date) AS first_sale_date, SUM(revenue) AS total_revenue FROM store_sales GROUP BY product_id ORDER BY total_revenue
