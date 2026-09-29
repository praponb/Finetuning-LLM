SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, YEAR(opened_date) AS opened_year, COUNT(sqft) AS sqft_count, COUNT(*) AS store_count FROM stores GROUP BY 1, 2 ORDER BY opened_month;
