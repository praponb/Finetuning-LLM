SELECT YEAR(opened_date) opened_year, MAX(opened_date) latest_opened_date, MIN(sqft) min_sqft, AVG(sqft) avg_sqft FROM stores WHERE sqft <= 10672 GROUP BY YEAR(opened_date) HAVING MIN(sqft) < 9100;
