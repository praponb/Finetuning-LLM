SELECT
    CASE WHEN country IN ('DE', 'SG') THEN 'priority' ELSE 'other' END AS country_group,
    CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 1.8 THEN 'mid' ELSE 'low' END AS rating_bucket,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM suppliers
GROUP BY 1, 2
HAVING us_count >= 3
ORDER BY rating_bucket DESC, country_group
LIMIT 5;
