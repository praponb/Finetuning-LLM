SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values
FROM suppliers
WHERE rating < 3.6 OR rating > 3.1
ORDER BY country_values ASC;
