WITH recent_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE city = 'New York'
),
Summary AS (
    SELECT
        city,
        country,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        MAX(rating) - MIN(rating) AS rating_range
    FROM recent_suppliers
    GROUP BY city, country
)
SELECT
    city,
    country,
    city_list,
    rating_range
FROM Summary
ORDER BY country
