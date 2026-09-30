SELECT country, ROUND(STDDEV(rating), 2) AS stddev_rating FROM suppliers GROUP BY country;
