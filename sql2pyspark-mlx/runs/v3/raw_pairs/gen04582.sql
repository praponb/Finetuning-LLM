SELECT
    country,
    CASE WHEN rating >= 4.2 THEN 'high' WHEN rating >= 1.8 THEN 'mid' ELSE 'low' END AS rating_bucket,
    city,
    COUNT(rating) AS rating_count
FROM suppliers
GROUP BY country, rating_bucket, city
HAVING COUNT(rating) < 1
