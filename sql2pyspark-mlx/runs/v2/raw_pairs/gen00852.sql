SELECT location, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list FROM departments WHERE dept_name = 'Legal' AND dept_name IN ('Sales', 'Engineering', 'HR') GROUP BY location ORDER BY location
