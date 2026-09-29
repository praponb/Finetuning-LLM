SELECT region, city, MIN(sqft) AS min_sqft, MAX(opened_date) AS latest_opened_date FROM stores GROUP BY region, city
