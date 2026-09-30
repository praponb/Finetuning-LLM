SELECT is_active, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values FROM customers GROUP BY is_active ORDER BY country_values
