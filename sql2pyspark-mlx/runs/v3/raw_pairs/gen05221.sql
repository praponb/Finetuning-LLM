SELECT ROUND(STDDEV(rating), 2) AS stddev_rating FROM suppliers WHERE rating >= 4.2 ORDER BY stddev_rating DESC;
