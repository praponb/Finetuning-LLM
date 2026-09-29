SELECT city, MIN(rating) AS min_rating
FROM suppliers
GROUP BY city
HAVING min_rating > 2.2;
