SELECT country, city, rating, ROUND(rating, 0) AS rating_rounded, CONCAT(city, ' ', country) AS city_country FROM suppliers ORDER BY city DESC
