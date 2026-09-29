SELECT city, COUNT(DISTINCT rating) AS unique_countrys FROM suppliers GROUP BY city HAVING unique_countrys > 3 ORDER BY unique_countrys;
