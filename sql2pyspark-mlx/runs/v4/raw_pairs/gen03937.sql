SELECT
    country,
    COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count,
    ROUND(AVG(rating), 2) AS avg_rating,
    SUM(rating) AS total_rating
FROM suppliers
WHERE city = 'New York' AND country = 'UK' AND rating BETWEEN 1.9 AND 4.0
GROUP BY 1;
