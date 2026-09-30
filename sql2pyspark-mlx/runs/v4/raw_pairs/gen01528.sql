SELECT
    CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket,
    city,
    SUM(CASE WHEN city = 'Chiang Mai' THEN rating ELSE 0 END) AS chiang_mai_rating,
    COUNT(rating) AS rating_count,
    SUM(rating) AS total_rating
FROM suppliers
GROUP BY rating_bucket, city
HAVING SUM(rating) > 20
ORDER BY rating_bucket;
