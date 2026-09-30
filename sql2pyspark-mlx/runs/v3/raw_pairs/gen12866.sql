SELECT s.region, SUM(ss.units) AS total_units, ROUND(STDDEV(ss.revenue), 2) AS stddev_revenue FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id GROUP BY s.region
