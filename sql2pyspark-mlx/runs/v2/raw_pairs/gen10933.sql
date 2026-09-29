SELECT city, ROUND(STDDEV(sqft), 2) AS stddev_sqft, COUNT(*) AS store_count FROM stores WHERE store_name LIKE 'Si%' AND city != 'Bangkok' GROUP BY city HAVING COUNT(*) < 4 ORDER BY city LIMIT 10;
