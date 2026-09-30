SELECT city, SUM(CASE WHEN country = 'UK' THEN rating ELSE 0 END) uk_rating, ROUND(AVG(rating), 2) avg_rating FROM suppliers GROUP BY city ORDER BY avg_rating DESC;
