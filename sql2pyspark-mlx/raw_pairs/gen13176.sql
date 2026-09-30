SELECT city, region, ROUND(STDDEV(sqft), 2) AS stddev_sqft, COUNT(DISTINCT sqft) AS unique_citys FROM stores WHERE sqft <= 13992 GROUP BY city, region ORDER BY city;
