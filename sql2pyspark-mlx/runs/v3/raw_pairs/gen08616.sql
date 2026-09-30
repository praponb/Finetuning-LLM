SELECT id, manager_id, hire_date, level, ROUND(salary, 1) AS salary_rounded FROM employees WHERE is_remote = TRUE OR level = 'L3'
