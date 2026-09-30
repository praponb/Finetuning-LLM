SELECT location, MIN(budget) AS min_budget, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM departments
WHERE dept_name <> 'HR' AND budget IS NULL
GROUP BY 1;
