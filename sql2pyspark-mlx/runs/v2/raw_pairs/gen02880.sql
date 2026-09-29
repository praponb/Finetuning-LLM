SELECT region, YEAR(opened_date) AS opened_year, MIN(opened_date) AS min_opened_date FROM stores WHERE sqft > 11040 GROUP BY region, YEAR(opened_date) ORDER BY 3
