SELECT city, ROUND(AVG(rating), 2) avg_rating, COUNT(DISTINCT rating) unique_citys, SUM(CASE WHEN city = 'Tokyo' THEN rating ELSE 0 END) tokyo_rating FROM suppliers GROUP BY city ORDER BY city DESC
