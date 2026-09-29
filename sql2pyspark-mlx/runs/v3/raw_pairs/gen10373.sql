SELECT city, COUNT(CASE WHEN city = 'Berlin' THEN 1 END) AS berlin_count, MAX(age) AS max_age FROM customers WHERE segment IN ('SMB', 'Consumer') GROUP BY city;
