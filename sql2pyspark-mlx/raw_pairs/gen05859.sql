SELECT id, product_id, sale_date, revenue, SUM(revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id) running_revenue FROM store_sales;
