SELECT SORT_ARRAY(COLLECT_SET(level)) AS level_list, MAX(salary) AS max_salary, MAX(salary) - MIN(salary) AS salary_range FROM employees
