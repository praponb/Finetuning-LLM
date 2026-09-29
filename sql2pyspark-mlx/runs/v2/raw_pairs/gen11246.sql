SELECT QUARTER(opened_date) AS opened_quarter, YEAR(opened_date) AS opened_year, MAX(sqft) AS max_sqft FROM stores GROUP BY opened_quarter, opened_year ORDER BY opened_year
