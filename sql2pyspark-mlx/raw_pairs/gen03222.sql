SELECT SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values FROM departments WHERE location IS NULL
