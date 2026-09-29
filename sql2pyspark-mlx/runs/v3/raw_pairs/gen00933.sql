SELECT id, revenue, ROUND(revenue / units, 2) revenue_per_unit FROM store_sales WHERE MONTH(sale_date) IN (1, 3, 10) AND revenue < 2500 ORDER BY revenue_per_unit DESC, id LIMIT 3
