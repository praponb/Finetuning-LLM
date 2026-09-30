SELECT
    CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 1.4 THEN 'mid' ELSE 'low' END AS rating_bucket,
    city,
    ROUND(AVG(rating), 2) AS avg_rating,
    SUM(rating) AS total_rating
FROM suppliers
WHERE city IN ('Tokyo', 'Chiang Mai')
GROUP BY 1, 2
ORDER BY city;
