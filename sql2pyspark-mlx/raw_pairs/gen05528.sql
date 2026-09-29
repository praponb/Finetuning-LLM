SELECT *
FROM (
    SELECT
        CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END AS rating_bucket,
        country,
        COUNT(rating) AS rating_count,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list
    FROM suppliers
    WHERE rating < 1.8
    GROUP BY CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END, country
) AS x
WHERE rating_count > 4
