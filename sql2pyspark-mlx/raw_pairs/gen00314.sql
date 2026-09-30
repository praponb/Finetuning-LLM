SELECT QUARTER(opened_date) AS opened_quarter, MIN(sqft) AS min_sqft FROM stores GROUP BY QUARTER(opened_date) ORDER BY min_sqft;
