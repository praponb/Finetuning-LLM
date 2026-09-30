SELECT rating_bucket, total_rating
FROM (
    SELECT CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(rating) AS total_rating
    FROM suppliers
    WHERE rating > 3.9
    GROUP BY CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END
) AS t
WHERE total_rating > 12
