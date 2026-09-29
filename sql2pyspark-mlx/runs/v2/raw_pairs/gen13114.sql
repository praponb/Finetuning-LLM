SELECT region, ROUND(AVG(sqft), 2) AS avg_sqft, SUM(CASE WHEN region = 'East' THEN sqft ELSE 0 END) AS east_sqft FROM stores GROUP BY 1 ORDER BY avg_sqft ASC
