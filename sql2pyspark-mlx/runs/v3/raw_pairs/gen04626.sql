SELECT location, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM departments
GROUP BY location
