SELECT * FROM (SELECT city, SUM(CASE WHEN city = 'Berlin' THEN sqft ELSE 0 END) AS berlin_sqft FROM stores GROUP BY city) AS sub WHERE berlin_sqft < 47 ORDER BY city
