SELECT city, MAX(rating) max_rating FROM suppliers GROUP BY city HAVING MAX(rating) >= 2.3 ORDER BY city
