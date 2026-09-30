SELECT
    city,
    CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END AS rating_bucket,
    SUM(CASE WHEN country = 'US' THEN 1 ELSE 0 END) AS us_count,
    COUNT(DISTINCT rating) AS unique_citys
FROM suppliers
WHERE rating < 2.6
GROUP BY 1, 2
