SELECT YEAR(opened_date) AS opened_year, region, SUM(sqft) AS total_sqft FROM stores WHERE city = 'Tokyo' GROUP BY YEAR(opened_date), region HAVING SUM(sqft) < 56000 ORDER BY region, total_sqft DESC;
