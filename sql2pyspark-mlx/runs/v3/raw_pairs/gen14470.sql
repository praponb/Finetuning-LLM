SELECT country, ROUND(STDDEV(rating), 2) AS stddev_rating, ROUND(AVG(rating), 2) AS avg_rating
FROM suppliers
WHERE city = 'Singapore' AND country = 'US' AND rating BETWEEN 2.3 AND 3.4
GROUP BY 1
ORDER BY stddev_rating, avg_rating DESC
