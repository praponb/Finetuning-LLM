SELECT YEAR(opened_date) AS opened_year, region, SUM(CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END) AS singapore_count FROM stores GROUP BY CUBE(YEAR(opened_date), region)
