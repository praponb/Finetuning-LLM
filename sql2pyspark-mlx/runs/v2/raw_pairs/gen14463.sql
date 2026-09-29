SELECT *
FROM (
    SELECT city, ROUND(AVG(rating), 2) avg_rating, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) city_values
    FROM suppliers
    GROUP BY city
) x
WHERE avg_rating > 29
