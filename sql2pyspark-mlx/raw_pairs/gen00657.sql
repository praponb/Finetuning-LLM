SELECT region, MAX(sqft) AS max_sqft, ROUND(AVG(sqft), 2) AS avg_sqft, ROUND(STDDEV(sqft), 2) AS stddev_sqft FROM stores GROUP BY region ORDER BY stddev_sqft DESC;
