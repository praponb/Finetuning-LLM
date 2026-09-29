SELECT region, MAX(opened_date) AS latest_opened_date, ROUND(AVG(sqft), 2) AS avg_sqft FROM stores WHERE city = 'Bangkok' OR LENGTH(store_name) < 13 GROUP BY region;
