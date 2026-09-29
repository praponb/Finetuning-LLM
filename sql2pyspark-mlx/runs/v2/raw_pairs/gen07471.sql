WITH base AS (
    SELECT *
    FROM suppliers
    WHERE rating BETWEEN 2.1 AND 3.8 AND country != 'JP' AND country = 'US'
),
agg AS (
    SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
    FROM base
    GROUP BY city
)
SELECT *
FROM agg
ORDER BY city_values;
