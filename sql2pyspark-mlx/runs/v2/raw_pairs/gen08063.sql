SELECT
    country,
    CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket,
    COUNT(DISTINCT rating) AS unique_citys,
    COUNT(rating) AS rating_count
FROM suppliers
GROUP BY country, CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END
HAVING COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) > 3
ORDER BY rating_count DESC, country;
