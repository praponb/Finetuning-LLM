SELECT country, ROUND(rating * 1.07, 2) AS rating_scaled, ROUND(rating * 100, 2) AS rating_scaled_2 FROM suppliers WHERE rating > 2.9 ORDER BY country DESC
