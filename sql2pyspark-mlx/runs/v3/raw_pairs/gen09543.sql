SELECT QUARTER(opened_date) AS opened_quarter, MIN(sqft) AS min_sqft, COUNT(*) AS cnt FROM stores WHERE city = 'New York' AND city = 'Berlin' GROUP BY 1 HAVING min_sqft < 12400;
