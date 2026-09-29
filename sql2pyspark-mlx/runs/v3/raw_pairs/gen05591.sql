SELECT dept_name, location, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) location_values
FROM departments
GROUP BY dept_name, location
