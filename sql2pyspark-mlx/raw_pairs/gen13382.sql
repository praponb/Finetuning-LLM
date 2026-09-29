WITH filtered_suppliers AS (
    SELECT *, CONCAT(country, '/', supplier_name) AS country_supplier_name
    FROM suppliers
    WHERE city <> 'Berlin' OR rating <= 3.9
),
summary AS (
    SELECT country, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
    FROM filtered_suppliers
    GROUP BY country
)
SELECT country, city_values
FROM summary
ORDER BY city_values;
