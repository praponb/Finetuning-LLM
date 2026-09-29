SELECT city, avg_sqft, min_sqft FROM (SELECT city, ROUND(AVG(sqft), 2) avg_sqft, MIN(sqft) min_sqft FROM stores WHERE sqft < 12349 GROUP BY city) agg WHERE min_sqft < 6200;
