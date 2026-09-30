SELECT id, store_id, sale_date, revenue, LAG(revenue, 1) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS prev_revenue FROM store_sales WHERE units IS NULL ORDER BY 2 DESC;
