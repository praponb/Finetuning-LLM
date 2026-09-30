SELECT s.city, SUM(ss.revenue) total_revenue, COUNT(*) num_store_sales FROM store_sales ss LEFT OUTER JOIN stores s ON ss.store_id = s.id GROUP BY s.city
