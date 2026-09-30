SELECT
    CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket,
    city,
    MAX(rating) - MIN(rating) AS rating_range,
    MIN(rating) AS min_rating
FROM suppliers
WHERE rating >= 1.9 OR rating > 4.0
GROUP BY 1, 2
