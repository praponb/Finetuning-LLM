SELECT city, YEAR(opened_date) AS opened_year, MAX(sqft) - MIN(sqft) AS sqft_range
FROM stores
GROUP BY city, YEAR(opened_date)
ORDER BY opened_year DESC
