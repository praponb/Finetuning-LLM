SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values
FROM suppliers
ORDER BY 1 ASC
