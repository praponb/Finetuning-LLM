SELECT city, SUM(rating) AS total_rating, DENSE_RANK() OVER (ORDER BY SUM(rating) DESC) AS rnk FROM suppliers WHERE rating >= 4.1 GROUP BY city ORDER BY rnk DESC, city LIMIT 10;
