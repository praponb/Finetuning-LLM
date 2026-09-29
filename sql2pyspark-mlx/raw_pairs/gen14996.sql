SELECT store_id, SUM(units) AS total_units, COUNT(*) AS cnt, SUM(SUM(units)) OVER (ORDER BY store_id) AS cumulative_units FROM store_sales GROUP BY store_id ORDER BY 4 DESC, store_id ASC LIMIT 10
