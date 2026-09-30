SELECT
    CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 2.6 THEN 'mid' ELSE 'low' END AS rating_bucket,
    CASE WHEN city IN ('Berlin', 'Tokyo') THEN 'group_a' ELSE 'other' END AS city_group,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values,
    SUM(rating) AS total_rating,
    ROUND(STDDEV(rating), 2) AS stddev_rating
FROM suppliers
GROUP BY 1, 2
ORDER BY total_rating DESC
