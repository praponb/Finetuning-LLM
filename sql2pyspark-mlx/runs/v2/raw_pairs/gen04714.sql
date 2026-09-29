SELECT id, city, rating, SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating FROM suppliers WHERE rating BETWEEN 2.1 AND 4.2
