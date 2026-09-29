SELECT location, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) location_values FROM departments GROUP BY location ORDER BY 1;
