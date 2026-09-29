SELECT city, COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count, MIN(opened_date) AS first_opened_date FROM stores GROUP BY city HAVING MIN(sqft) >= 7600 ORDER BY city;
