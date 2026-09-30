SELECT id, sale_date, revenue, product_id, LEAD(revenue, 1) OVER (ORDER BY sale_date, id) AS next_revenue, RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) AS rnk FROM store_sales;
