SELECT region, MIN(opened_date) AS min_opened_date, SUM(sqft) AS total_sqft, COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count FROM stores GROUP BY region;
