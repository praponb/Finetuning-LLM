SELECT city, ROUND(STDDEV(rating), 2) stddev_rating FROM suppliers WHERE rating > 3.5 AND rating > 3.1 AND rating > 2.2 GROUP BY city HAVING stddev_rating >= 50;
