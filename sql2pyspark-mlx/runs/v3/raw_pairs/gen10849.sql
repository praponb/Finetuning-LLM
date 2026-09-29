SELECT CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(CASE WHEN country = 'UK' THEN rating ELSE 0 END) AS uk_rating, ROUND(AVG(rating), 2) AS avg_rating
FROM suppliers
WHERE city IN ('Berlin', 'Tokyo') AND country <> 'UK'
GROUP BY rating_bucket
ORDER BY 2 DESC, rating_bucket;
