SELECT
    country,
    COUNT(rating) AS rating_count,
    COUNT(*) AS cnt,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
FROM suppliers
WHERE rating <= 3.3 AND supplier_name LIKE 'Glo%'
GROUP BY 1;
