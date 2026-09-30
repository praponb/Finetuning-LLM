SELECT id, opened_date, region, city, UPPER(region) AS region_upper FROM stores WHERE sqft >= 9151 AND sqft > 8726 AND sqft = 10315 ORDER BY city ASC
