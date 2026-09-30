SELECT
    CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket,
    city,
    country,
    COUNT(DISTINCT rating) AS unique_countrys,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count
FROM suppliers
GROUP BY CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END, city, country
HAVING COUNT(CASE WHEN country = 'US' THEN 1 END) < 2
ORDER BY country ASC;
