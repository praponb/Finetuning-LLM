SELECT country, SUM(CASE WHEN city = 'Tokyo' THEN rating ELSE 0 END) AS tokyo_rating
FROM suppliers
GROUP BY country
ORDER BY tokyo_rating
