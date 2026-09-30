SELECT
    CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END AS rating_bucket,
    ROUND(AVG(rating), 2) AS avg_rating,
    COUNT(DISTINCT rating) AS unique_countrys,
    SUM(CASE WHEN country = 'UK' THEN rating ELSE 0 END) AS uk_rating
FROM suppliers
WHERE rating BETWEEN 3.7 AND 4.0
GROUP BY 1;
