SELECT
    CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket,
    city,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values,
    SUM(rating) AS total_rating
FROM suppliers
WHERE country IN ('JP', 'TH', 'DE')
GROUP BY 1, 2
HAVING SUM(rating) >= 12
