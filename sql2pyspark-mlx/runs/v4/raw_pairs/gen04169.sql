SELECT MIN(opened_date) AS min_opened_date, MIN(sqft) AS min_sqft
FROM stores
WHERE store_name LIKE '%Tow%' OR city = 'Singapore'
ORDER BY min_opened_date
