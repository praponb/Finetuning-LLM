SELECT
    country,
    CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END rating_bucket,
    city,
    SORT_ARRAY(COLLECT_SET(city)) city_list,
    SORT_ARRAY(COLLECT_SET(country)) country_list
FROM suppliers
GROUP BY country, rating_bucket, city
ORDER BY rating_bucket ASC, country ASC, city ASC
LIMIT 5;
