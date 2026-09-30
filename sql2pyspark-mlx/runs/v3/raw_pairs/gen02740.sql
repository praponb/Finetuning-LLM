SELECT * FROM (SELECT city, SUM(sqft) AS total_sqft, MAX(sqft) AS max_sqft FROM stores GROUP BY city) AS agg WHERE total_sqft >= 29000
