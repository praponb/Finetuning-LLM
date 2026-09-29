SELECT
    city,
    CASE WHEN city IN ('Berlin', 'New York') THEN 'tier_1' ELSE 'other' END AS city_group,
    COUNT(rating) AS rating_count,
    SUM(CASE WHEN city = 'Bangkok' THEN rating ELSE 0 END) AS bangkok_rating,
    SUM(CASE WHEN city = 'Chiang Mai' THEN rating ELSE 0 END) AS chiang_mai_rating
FROM suppliers
WHERE LOWER(supplier_name) LIKE '%ion%'
GROUP BY 1, 2
HAVING COUNT(DISTINCT rating) > 5;
