SELECT
    city,
    CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM suppliers
WHERE rating <= 2.1
GROUP BY city, CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END
ORDER BY rating_bucket, city
LIMIT 3;
