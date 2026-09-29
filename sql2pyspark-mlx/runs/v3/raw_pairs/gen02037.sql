SELECT location, ROUND(AVG(budget), 2) AS avg_budget, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values FROM departments WHERE dept_name = 'HR' GROUP BY 1
