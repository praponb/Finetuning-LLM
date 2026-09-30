SELECT
    city,
    CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END AS rating_bucket,
    MAX(rating) AS max_rating,
    MAX(rating) - MIN(rating) AS rating_range,
    SUM(CASE WHEN city = 'Singapore' THEN rating ELSE 0 END) AS singapore_rating
FROM suppliers
WHERE country NOT IN ('DE', 'SG', 'US')
GROUP BY city, rating_bucket
HAVING MAX(rating) - MIN(rating) > 59;
