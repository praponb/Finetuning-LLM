WITH base AS (
    SELECT *
    FROM customers
    WHERE city IN ('Bangkok', 'New York') AND is_active
),
summary AS (
    SELECT city, COUNT(DISTINCT age) AS unique_countrys, SORT_ARRAY(COLLECT_SET(city)) AS city_list
    FROM base
    GROUP BY city
)
SELECT city, unique_countrys, city_list
FROM summary
ORDER BY unique_countrys
