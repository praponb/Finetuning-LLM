SELECT region, ROUND(STDDEV(sqft), 2) AS stddev_sqft, SUM(sqft) AS total_sqft FROM stores GROUP BY region;
