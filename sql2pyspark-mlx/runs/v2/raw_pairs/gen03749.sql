SELECT city, ROUND(STDDEV(sqft), 2) AS stddev_sqft FROM stores GROUP BY city;
