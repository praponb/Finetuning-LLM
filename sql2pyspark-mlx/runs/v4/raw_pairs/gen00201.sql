SELECT
    city,
    country,
    ROUND(STDDEV(rating), 2) AS stddev_rating,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM suppliers
WHERE city <> 'Bangkok' OR country IN ('UK', 'DE')
GROUP BY city, country
HAVING ROUND(STDDEV(rating), 2) >= 77;
