SELECT *
FROM (
    SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values, AVG(rating) AS avg_rating
    FROM suppliers
    WHERE supplier_name LIKE 'Eu%'
    GROUP BY city
) AS sub
WHERE avg_rating < 2.5
ORDER BY city_values;
