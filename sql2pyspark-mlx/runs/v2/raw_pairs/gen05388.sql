SELECT ROUND(AVG(budget), 2) AS avg_budget, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values FROM departments ORDER BY dept_name_values
