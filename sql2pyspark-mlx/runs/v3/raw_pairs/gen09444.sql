SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, SUM(sqft) AS total_sqft, MAX(sqft) - MIN(sqft) AS sqft_range FROM stores GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
