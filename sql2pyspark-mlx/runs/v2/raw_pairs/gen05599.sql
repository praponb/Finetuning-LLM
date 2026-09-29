SELECT city, SUM(CASE WHEN city = 'Tokyo' THEN rating ELSE 0 END) AS tokyo_rating, MAX(rating) AS max_rating
FROM suppliers
GROUP BY city
HAVING MAX(rating) < 2.9
ORDER BY tokyo_rating DESC;
