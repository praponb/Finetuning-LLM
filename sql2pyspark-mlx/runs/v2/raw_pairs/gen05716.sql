SELECT MONTH(opened_date) AS opened_month, MIN(opened_date) AS min_opened_date, MAX(sqft) - MIN(sqft) AS sqft_range FROM stores GROUP BY 1 ORDER BY min_opened_date, sqft_range DESC;
