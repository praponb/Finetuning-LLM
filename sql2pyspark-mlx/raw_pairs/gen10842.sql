SELECT
    country,
    rating_bucket,
    avg_rating,
    city_list
FROM (
    SELECT
        country,
        CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END AS rating_bucket,
        ROUND(AVG(rating), 2) AS avg_rating,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list
    FROM suppliers
    WHERE country = 'UK'
    GROUP BY country, CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END
) AS sub
WHERE avg_rating > 52
ORDER BY avg_rating ASC;
