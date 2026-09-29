SELECT dept_name, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list FROM departments GROUP BY 1;
