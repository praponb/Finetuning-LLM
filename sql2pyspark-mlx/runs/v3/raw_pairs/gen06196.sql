SELECT city, DATE_TRUNC('MONTH', opened_date) AS opened_month, MAX(sqft) AS max_sqft
FROM stores
GROUP BY city, DATE_TRUNC('MONTH', opened_date)
ORDER BY city;
