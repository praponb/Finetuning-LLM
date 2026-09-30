SELECT id, sale_date, revenue, product_id, LEAD(revenue, 1) OVER (ORDER BY sale_date, id) next_revenue, MAX(revenue) OVER (PARTITION BY product_id) product_id_max_revenue FROM store_sales;
