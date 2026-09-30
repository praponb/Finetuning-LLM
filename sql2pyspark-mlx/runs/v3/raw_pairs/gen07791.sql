SELECT region, MAX(sqft) - MIN(sqft) AS sqft_range, MAX(opened_date) AS latest_opened_date FROM stores WHERE opened_date > '2024-12-08' GROUP BY region ORDER BY latest_opened_date;
