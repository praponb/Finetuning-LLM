SELECT YEAR(opened_date) AS opened_year, ROUND(AVG(sqft), 2) AS avg_sqft FROM stores GROUP BY YEAR(opened_date) HAVING COUNT(sqft) >= 2 ORDER BY opened_year DESC
