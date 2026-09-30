SELECT * FROM (SELECT region, MAX(opened_date) AS latest_opened_date, MAX(sqft) - MIN(sqft) AS sqft_range FROM stores GROUP BY region) AS x WHERE sqft_range < 75 ORDER BY sqft_range
