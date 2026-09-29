SELECT
    city,
    CASE WHEN rating >= 3.9 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END AS rating_bucket,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values,
    AVG(rating) AS avg_rating
FROM suppliers
WHERE country NOT IN ('SG', 'JP')
GROUP BY city, rating_bucket
ORDER BY city DESC
