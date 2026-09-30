SELECT MONTH(opened_date) AS opened_month, YEAR(opened_date) AS opened_year, COUNT(sqft) AS sqft_count, MAX(sqft) - MIN(sqft) AS sqft_range FROM stores GROUP BY 1, 2;
