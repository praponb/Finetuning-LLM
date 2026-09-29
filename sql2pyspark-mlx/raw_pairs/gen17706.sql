SELECT
    country,
    MAX(rating) AS max_rating,
    ROUND((100 * SUM(CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_singapore,
    COUNT(DISTINCT rating) AS unique_citys
FROM suppliers
GROUP BY country
