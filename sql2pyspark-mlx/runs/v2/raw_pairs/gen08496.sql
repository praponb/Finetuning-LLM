SELECT country, city, MIN(rating) min_rating, COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) tokyo_count FROM suppliers WHERE country != 'UK' GROUP BY country, city ORDER BY country DESC
