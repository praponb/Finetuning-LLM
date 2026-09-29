SELECT region, opened_date, city, ROUND(sqft, 2) sqft_rounded FROM stores WHERE region = 'North' AND sqft = 13018 AND region = 'Central' ORDER BY opened_date DESC;
