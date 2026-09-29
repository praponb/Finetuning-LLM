SELECT country, SUM(CASE WHEN city = 'London' THEN rating ELSE 0 END) AS london_rating, ROUND(STDDEV(rating), 2) AS stddev_rating
FROM suppliers
WHERE country = 'JP'
GROUP BY country
ORDER BY country ASC
