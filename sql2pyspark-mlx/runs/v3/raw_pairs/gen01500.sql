SELECT location, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values, AVG(budget) AS avg_budget FROM departments GROUP BY location;
