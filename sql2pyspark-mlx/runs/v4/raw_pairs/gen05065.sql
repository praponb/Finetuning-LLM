SELECT CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(rating) AS total_rating
FROM suppliers
GROUP BY rating_bucket
HAVING SUM(rating) >= 23
ORDER BY total_rating, rating_bucket;
