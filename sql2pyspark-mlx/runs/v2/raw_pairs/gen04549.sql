SELECT city, DATE_FORMAT(opened_date, 'yyyy-MM') opened_month, MAX(sqft) - MIN(sqft) sqft_range
FROM stores
GROUP BY city, DATE_FORMAT(opened_date, 'yyyy-MM')
HAVING MAX(sqft) - MIN(sqft) < 20
