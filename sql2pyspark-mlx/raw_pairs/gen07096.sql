SELECT
    country,
    CASE WHEN age >= 67 THEN 'high' WHEN age >= 40 THEN 'mid' ELSE 'low' END AS age_bucket,
    AVG(age) AS avg_age,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM customers
WHERE city NOT IN ('Chiang Mai', 'New York')
GROUP BY country, CASE WHEN age >= 67 THEN 'high' WHEN age >= 40 THEN 'mid' ELSE 'low' END
