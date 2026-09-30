SELECT
    city,
    MAX(rating) max_rating,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) country_values,
    MIN(rating) min_rating
FROM suppliers
WHERE country = 'DE'
GROUP BY city
HAVING MAX(rating) >= 4.0
ORDER BY city
