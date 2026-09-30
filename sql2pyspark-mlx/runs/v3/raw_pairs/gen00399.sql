SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values
FROM departments
WHERE dept_name = 'Finance'
ORDER BY dept_name_values;
