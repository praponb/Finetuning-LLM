SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) dept_name_values
FROM departments
WHERE dept_name <> 'Legal'
ORDER BY dept_name_values DESC;
