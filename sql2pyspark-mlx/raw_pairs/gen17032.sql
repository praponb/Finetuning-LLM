SELECT id, product_id, sale_date, revenue, LEAD(revenue, 1) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS next_revenue FROM store_sales ORDER BY revenue DESC;
