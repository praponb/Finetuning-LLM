SELECT city, ROUND(STDDEV(sqft), 2) AS stddev_sqft, ROUND(AVG(sqft), 2) AS avg_sqft, MAX(sqft) - MIN(sqft) AS sqft_range FROM stores WHERE sqft > 10847 GROUP BY city;
