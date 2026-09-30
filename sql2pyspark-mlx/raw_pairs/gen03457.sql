WITH filtered_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE rating <= 1.9
),
agg AS (
    SELECT country, SORT_ARRAY(COLLECT_SET(city)) AS city_list
    FROM filtered_suppliers
    GROUP BY country
)
SELECT country, city_list
FROM agg;
