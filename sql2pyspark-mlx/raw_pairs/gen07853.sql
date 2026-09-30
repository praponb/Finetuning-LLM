SELECT country, SUM(CASE WHEN country = 'JP' THEN rating ELSE 0 END) AS jp_rating FROM suppliers GROUP BY country ORDER BY jp_rating DESC;
