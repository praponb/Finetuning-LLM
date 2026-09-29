SELECT dept_name, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values, COUNT(budget) AS budget_count FROM departments GROUP BY 1 ORDER BY dept_name_values DESC;
