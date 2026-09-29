WITH recent_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE (city IN ('Tokyo', 'Singapore', 'Berlin') AND country IN ('US', 'TH', 'SG')) OR city = 'Chiang Mai'
),
summary AS (
    SELECT CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
    FROM recent_suppliers
    GROUP BY CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END, city
)
SELECT rating_bucket, city, city_values
FROM summary
ORDER BY city_values DESC, city, rating_bucket
LIMIT 3;
