SELECT id, sale_date, revenue, SUM(revenue) OVER (ORDER BY sale_date, id) AS running_revenue FROM store_sales
