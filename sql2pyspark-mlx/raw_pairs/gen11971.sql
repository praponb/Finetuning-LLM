SELECT country, CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END AS rating_bucket, MIN(rating) AS min_rating
FROM suppliers
WHERE city != 'London'
GROUP BY country, rating_bucket
