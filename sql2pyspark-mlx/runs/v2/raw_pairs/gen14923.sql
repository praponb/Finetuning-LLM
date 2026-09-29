SELECT is_remote, SUM(salary) total_salary, SORT_ARRAY(COLLECT_SET(level)) level_list
FROM employees
WHERE hire_date < '2024-05-01'
GROUP BY 1;
