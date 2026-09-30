SELECT COUNT(budget) AS budget_count, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM departments
WHERE location = 'Singapore' AND dept_name NOT IN ('Marketing', 'Finance', 'HR')
ORDER BY budget_count;
