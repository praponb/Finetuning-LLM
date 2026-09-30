SELECT country, CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END rating_bucket, ROUND(AVG(rating), 2) avg_rating
FROM suppliers
GROUP BY country, CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END
HAVING ROUND(AVG(rating), 2) >= 30
ORDER BY rating_bucket
