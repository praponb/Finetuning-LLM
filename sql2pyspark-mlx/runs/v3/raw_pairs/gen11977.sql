SELECT MAX(opened_date) AS latest_opened_date, COUNT(sqft) AS sqft_count FROM stores ORDER BY sqft_count DESC, latest_opened_date;
