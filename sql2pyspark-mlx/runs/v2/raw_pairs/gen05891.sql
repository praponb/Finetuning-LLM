SELECT s.region, ROUND(AVG(ss.revenue), 2) AS avg_revenue FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id GROUP BY s.region;
