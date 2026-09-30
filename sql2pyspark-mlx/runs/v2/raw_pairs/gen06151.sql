SELECT AVG(salary) AS avg_salary, SORT_ARRAY(COLLECT_SET(level)) AS level_list FROM employees WHERE salary <= 113000
