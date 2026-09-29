SELECT YEAR(opened_date) AS opened_year, MONTH(opened_date) AS opened_month, SUM(sqft) AS total_sqft FROM stores WHERE store_name LIKE '%Old%' GROUP BY 1, 2
