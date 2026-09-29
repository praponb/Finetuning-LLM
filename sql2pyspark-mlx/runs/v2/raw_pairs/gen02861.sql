SELECT
    city,
    country,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values,
    MIN(rating) AS min_rating
FROM suppliers
GROUP BY city, country
ORDER BY country
