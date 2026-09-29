SELECT region, QUARTER(opened_date) AS opened_quarter, SUM(CASE WHEN region = 'North' THEN sqft ELSE 0 END) AS north_sqft
FROM stores
GROUP BY region, opened_quarter
HAVING MIN(sqft) >= 15500
ORDER BY opened_quarter DESC;
