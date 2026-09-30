SELECT id, product_id, sale_date, revenue, LEAD(revenue, 1) OVER (PARTITION BY product_id ORDER BY sale_date, id) next_revenue FROM store_sales ORDER BY sale_date
