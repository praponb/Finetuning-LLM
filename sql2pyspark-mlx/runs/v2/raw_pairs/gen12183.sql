SELECT country, rating_range, avg_rating FROM (SELECT country, MAX(rating) - MIN(rating) rating_range, AVG(rating) avg_rating FROM suppliers GROUP BY country) agg WHERE avg_rating > 4.1
