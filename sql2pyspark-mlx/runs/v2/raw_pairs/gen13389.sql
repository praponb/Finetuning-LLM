WITH src AS (
    SELECT *, CONCAT_WS('-', city, country) AS city_country_key
    FROM suppliers
    WHERE city IN ('Tokyo', 'Bangkok', 'Berlin')
),
city_summary AS (
    SELECT city, SORT_ARRAY(COLLECT_SET(country)) AS country_list
    FROM src
    GROUP BY city
)
SELECT city, country_list
FROM city_summary
ORDER BY city DESC;
