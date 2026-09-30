SELECT country, ROUND(STDDEV(rating), 2) AS stddev_rating
FROM suppliers
GROUP BY 1
ORDER BY stddev_rating DESC, country
LIMIT 5;
