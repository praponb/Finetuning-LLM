SELECT city, DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, SUM(CASE WHEN region = 'West' THEN sqft ELSE 0 END) AS west_sqft FROM stores GROUP BY city, opened_month ORDER BY west_sqft
