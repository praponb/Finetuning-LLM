SELECT city, SUM(rating) AS total_rating, DENSE_RANK() OVER (ORDER BY SUM(rating) DESC) AS rnk FROM suppliers WHERE city = 'Bangkok' GROUP BY city ORDER BY total_rating DESC, city DESC;
