SELECT CASE WHEN rating >= 3.9 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END AS rating_bucket, ROUND(AVG(rating), 2) AS avg_rating
FROM suppliers
GROUP BY rating_bucket
HAVING ROUND(AVG(rating), 2) < 79
ORDER BY rating_bucket;
