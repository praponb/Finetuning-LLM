SELECT segment, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values FROM customers WHERE is_active GROUP BY segment;
