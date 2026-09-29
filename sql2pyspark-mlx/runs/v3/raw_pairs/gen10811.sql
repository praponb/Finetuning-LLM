SELECT
    CASE WHEN rating >= 4.2 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket,
    country,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    MIN(rating) AS min_rating
FROM suppliers
GROUP BY CASE WHEN rating >= 4.2 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END, country
ORDER BY country
